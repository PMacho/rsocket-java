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

    protected Consumer<List<S>> rSocketListConsumer;
    protected Consumer<Long> updateConsumer;

    private final Flux<List<S>> hotRSocketsSource;

    private final AtomicReference<Disposable> state;
    private final DirectProcessor<Void> control = DirectProcessor.create();

    public RSocketPool(
            Publisher<? extends Collection<RSocket>> rSocketsPublisher,
            Function<RSocket, S> rSocketMapper,
            Function<List<S>, Mono<List<S>>> rSocketSortingStrategy
    ) {
        hotRSocketsSource = createHotRSocketsSource();
        state = new AtomicReference<>(start(rSocketsPublisher, rSocketMapper, rSocketSortingStrategy));
    }

    private Flux<List<S>> createHotRSocketsSource() {
        return Flux
                .<List<S>>create(sink -> rSocketListConsumer = sink::next)
                .share()
                .cache(1)
                .takeUntilOther(control);
    }

    public void clean() {
        logger.info("Cleaning RSocket pool.");
        control.onComplete();
        state.get().dispose();
    }

    public Disposable start(
            Publisher<? extends Collection<RSocket>> rSocketsPublisher,
            Function<RSocket, S> rSocketMapper,
            Function<List<S>, Mono<List<S>>> sortRSockets
    ) {
        logger.info("Starting RSocket pool.");
        return Flux
                .from(rSocketsPublisher)
                .distinctUntilChanged()
                .flatMap(sList(rSocketMapper))
                .switchMap(rSockets -> Flux
                        .merge(
                                Flux.interval(Duration.ZERO, Duration.ofMillis(2000)),
                                Flux.create(sink -> updateConsumer = sink::next)
                        )
                        .flatMap(i -> sortRSockets.apply(rSockets))
                )
                .doOnNext(rSockets -> rSocketListConsumer.accept(rSockets))
                .onErrorContinue((throwable, o) -> logger.error("Received error signal for " + o, throwable))
                .subscribe();
    }

    private Function<Collection<RSocket>, Mono<List<S>>> sList(Function<RSocket, S> rSocketMapper) {
        return rSockets -> Flux.fromIterable(rSockets).map(rSocketMapper).collectList();
    }

    public Mono<S> select() {
        return Flux
                .from(hotRSocketsSource)
                .next()
                .map(list -> list.get(0))
                .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofMillis(10)));
    }

    public void update() {
        updateConsumer.accept(0L);
    }

}
