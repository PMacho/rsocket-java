package io.rsocket.client.new_classes;

import io.rsocket.RSocket;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class RSocketPoolStatic<S extends RSocket> implements RSocketPool<S> {

    Logger logger = LoggerFactory.getLogger(RSocketPoolStatic.class);

    private static final Duration DEFAULT_MAX_REFRESH_DURATION = Duration.ofSeconds(5);

    protected Consumer<List<S>> rSocketListConsumer;
    protected Consumer<Long> updateConsumer;

    private final Flux<List<S>> activeRSockets;

    private AtomicReference<Disposable> poolAvailable = new AtomicReference<>();
    private AtomicReference<Disposable> poolState = new AtomicReference<>();
    private final DirectProcessor<Void> rSocketSourceControl = DirectProcessor.create();

    public RSocketPoolStatic() {
        activeRSockets = createActiveRSockets();
    }

    private Flux<List<S>> createActiveRSockets() {
        return Flux
                .<List<S>>create(sink -> rSocketListConsumer = sink::next)
                .share()
                .cache(1)
                .takeUntilOther(rSocketSourceControl);
    }

    protected void start(
            Publisher<? extends Collection<RSocket>> rSocketsPublisher,
            Function<RSocket, S> rSocketMapper,
            Function<List<S>, Mono<List<S>>> orderRSockets
    ) {
        logger.info("Starting RSocket pool.");
        poolAvailable.set(availablePoolUpdater(rSocketsPublisher, rSocketMapper));
        poolState.set(periodicAndTriggeredUpdater(orderRSockets));
    }

    private Disposable availablePoolUpdater(
            Publisher<? extends Collection<RSocket>> rSocketsPublisher,
            Function<RSocket, S> rSocketMapper
    ) {
        return Flux
                .from(rSocketsPublisher)
                .distinctUntilChanged()
                .flatMap(socketList(rSocketMapper))
                .as(this::poolUpdater);
    }

    private Function<Collection<RSocket>, Mono<List<S>>> socketList(Function<RSocket, S> rSocketMapper) {
        return rSockets -> Flux.fromIterable(rSockets).map(rSocketMapper).collectList();
    }

    private Disposable periodicAndTriggeredUpdater(
            Function<List<S>, Mono<List<S>>> orderRSockets
    ) {
        return Flux
                .merge(
                        Flux.interval(Duration.ZERO, DEFAULT_MAX_REFRESH_DURATION),
                        Flux.create(sink -> updateConsumer = sink::next)
                )
                .flatMap(i -> snapshot(activeRSockets))
                .flatMap(orderRSockets)
                .as(this::poolUpdater);
    }

    private Disposable poolUpdater(Flux<List<S>> flux) {
        return flux
                .onErrorContinue((throwable, o) -> logger.error("Received error signal for " + o, throwable))
                .subscribe(rSockets -> rSocketListConsumer.accept(rSockets));
    }

    @Override
    public Mono<S> select() {
        return Flux
                .from(activeRSockets)
                .next()
                .map(list -> list.get(0))
                .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofMillis(10)));
    }

    @Override
    public Flux<S> selectAll() {
        return Flux
                .from(activeRSockets)
                .next()
                .flatMapMany(Flux::fromIterable)
                .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofMillis(10)));
    }

    @Override
    public void update() {
        updateConsumer.accept(0L);
    }

    @Override
    public Mono<Void> clean() {
        logger.info("Cleaning RSocket pool.");
        return selectAll()
                .doOnNext(Disposable::dispose)
                .then(Mono.fromRunnable(() -> {
                    rSocketSourceControl.onComplete();
                    poolAvailable.get().dispose();
                    poolState.get().dispose();
                }));
    }

}
