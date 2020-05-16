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

public abstract class RSocketPool<S extends RSocket> {

    Logger logger = LoggerFactory.getLogger(RSocketPool.class);

    private static final Duration DEFAULT_MAX_REFRESH_DURATION = Duration.ofSeconds(5);

    protected Consumer<List<S>> rSocketListConsumer;
    protected Consumer<Long> updateConsumer;

    private final Flux<List<S>> hotRSocketsSource;

    private AtomicReference<Disposable> poolState = new AtomicReference<>();
    private final DirectProcessor<Void> control = DirectProcessor.create();

    public RSocketPool() {
        hotRSocketsSource = createHotRSocketsSource();
    }

    private Flux<List<S>> createHotRSocketsSource() {
        return Flux
                .<List<S>>create(sink -> rSocketListConsumer = sink::next)
                .share()
                .cache(1)
                .takeUntilOther(control);
    }

    protected abstract void start(Publisher<? extends Collection<RSocket>> rSocketsPublisher);

    public void start(
            Publisher<? extends Collection<RSocket>> rSocketsPublisher,
            Function<RSocket, S> rSocketMapper,
            Function<List<S>, Mono<List<S>>> orderRSockets
    ) {
        logger.info("Starting RSocket pool.");
        poolState.set(
                Flux
                        .from(rSocketsPublisher)
                        .distinctUntilChanged()
                        .flatMap(socketList(rSocketMapper))
//                        .subscribe(rSockets -> rSocketListConsumer.accept(rSockets));
//        Flux
//                .from(hotRSocketsSource)
                .switchMap(periodicAndTriggeredUpdater(orderRSockets))
                .onErrorContinue((throwable, o) -> logger.error("Received error signal for " + o, throwable))
                .subscribe(rSockets -> rSocketListConsumer.accept(rSockets))
        );
    }

    private Function<Collection<RSocket>, Mono<List<S>>> socketList(Function<RSocket, S> rSocketMapper) {
        return rSockets -> Flux.fromIterable(rSockets).map(rSocketMapper).collectList();
    }

    private Function<? super List<S>, Publisher<? extends List<S>>> periodicAndTriggeredUpdater(
            Function<List<S>, Mono<List<S>>> orderRSockets
    ) {
        return rSockets -> Flux
                .merge(
                        Flux.interval(Duration.ZERO, DEFAULT_MAX_REFRESH_DURATION),
                        Flux.create(sink -> updateConsumer = sink::next)
                )
                .flatMap(i -> orderRSockets.apply(rSockets));
    }

    public Mono<S> select() {
        return Flux
                .from(hotRSocketsSource)
                .next()
                .map(list -> list.get(0))
                .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofMillis(10)));
    }

    public Flux<S> selectAll() {
        return Flux
                .from(hotRSocketsSource)
                .next()
                .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofMillis(10)))
                .flatMapMany(Flux::fromIterable);
    }

    public void update() {
        updateConsumer.accept(0L);
    }

    public Mono<Void> clean() {
        logger.info("Cleaning RSocket pool.");
        return selectAll()
                .doOnNext(Disposable::dispose)
                .then(Mono.fromRunnable(() -> {
                    control.onComplete();
                    poolState.get().dispose();
                }));
    }

}
