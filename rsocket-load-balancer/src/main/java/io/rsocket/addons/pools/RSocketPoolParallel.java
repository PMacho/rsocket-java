package io.rsocket.addons.pools;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.addons.RSocketPool;
import io.rsocket.util.RSocketProxy;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class RSocketPoolParallel<S extends RSocket> implements RSocketPool, PoolOperations {

    Logger logger = LoggerFactory.getLogger(RSocketPoolParallel.class);

    private static final Duration DEFAULT_MAX_REFRESH_DURATION = Duration.ofSeconds(5);

    protected Consumer<List<S>> rSocketListConsumer;
    protected Consumer<Long> updateConsumer;

    private final Flux<List<S>> activeRSockets;
    // todo: initialize me correctly, or add error handling
    private final AtomicReference<S> next = new AtomicReference<>();

    private AtomicReference<Disposable> poolNext = new AtomicReference<>();
    private AtomicReference<Disposable> poolAvailable = new AtomicReference<>();
    private AtomicReference<Disposable> poolState = new AtomicReference<>();
//    private final DirectProcessor<Void> rSocketSourceControl = DirectProcessor.create();

    public RSocketPoolParallel(Publisher<? extends Collection<? extends RSocket>> rSocketsPublisher) {
        logger.info("Starting parallel RSocket pool.");
        activeRSockets = activeRSockets();
        poolNext.set(maintainNext());
        poolAvailable.set(availablePoolUpdater(rSocketsPublisher));
        poolState.set(periodicAndTriggeredUpdater());
    }

    protected abstract S rSocketMapper(RSocket rSocket);

    protected abstract Mono<List<S>> orderRSockets(List<S> sList);

    private Flux<List<S>> activeRSockets() {
        return Flux
                .<List<S>>create(sink -> rSocketListConsumer = sink::next)
                .as(this::hotSource);
    }

    private Disposable maintainNext() {
        return activeRSockets
                .map(list -> list.get(0))
                .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofMillis(10)))
                .subscribe(next::set);
    }

    private Disposable availablePoolUpdater(Publisher<? extends Collection<? extends RSocket>> rSocketsPublisher) {
        return Flux
                .from(rSocketsPublisher)
                .distinctUntilChanged()
                .flatMap(socketList())
                .as(this::poolUpdater);
    }

    private Function<Collection<? extends RSocket>, Mono<List<S>>> socketList() {
        return rSockets -> Flux
                .fromIterable(rSockets)
                .map(PooledRSocket::new)
                .map(this::rSocketMapper)
                .collectList();
    }

    private Disposable periodicAndTriggeredUpdater() {
        return Flux
                .merge(
                        Flux.interval(Duration.ZERO, DEFAULT_MAX_REFRESH_DURATION),
                        Flux.create(sink -> updateConsumer = sink::next)
                )
                .flatMap(i -> snapshot(activeRSockets))
                .flatMap(this::orderRSockets)
                .as(this::poolUpdater);
    }

    private Disposable poolUpdater(Flux<List<S>> flux) {
        return poolUpdater(flux, rSocketListConsumer);
    }

    @Override
    public RSocket select() {
        return next.get();
    }

    public void update() {
        updateConsumer.accept(0L);
    }

    @Override
    public Mono<Void> onClose() {
        logger.info("Cleaning RSocket pool.");
        return snapshot(activeRSockets)
                .flatMapMany(Flux::fromIterable)
                .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofMillis(10)))
                .doOnNext(Disposable::dispose)
                .then(Mono.fromRunnable(() -> {
                    sourceControl.onComplete();
                    poolNext.get().dispose();
                    poolAvailable.get().dispose();
                    poolState.get().dispose();
                }));
    }

    @Override
    // todo:
    public void dispose() {

    }

    private class PooledRSocket extends RSocketProxy {

        public PooledRSocket(RSocket source) {
            super(source);
        }

        @Override
        public Mono<Void> fireAndForget(Payload payload) {
            return source.fireAndForget(payload).doFinally(signalType -> update());
        }

        @Override
        public Mono<Payload> requestResponse(Payload payload) {
            return source.requestResponse(payload).doFinally(signalType -> update());
        }

        @Override
        public Flux<Payload> requestStream(Payload payload) {
            return source.requestStream(payload).doFinally(signalType -> update());
        }

        @Override
        public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            return source.requestChannel(payloads).doFinally(signalType -> update());
        }

        @Override
        public Mono<Void> metadataPush(Payload payload) {
            return source.metadataPush(payload).doFinally(signalType -> update());
        }

    }

}
