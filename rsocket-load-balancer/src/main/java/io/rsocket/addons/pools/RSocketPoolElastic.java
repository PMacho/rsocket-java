package io.rsocket.addons.pools;

import io.rsocket.RSocket;
import io.rsocket.addons.AsyncBaseRSocketPool;
import io.rsocket.addons.RSocketPool;
import io.rsocket.addons.ResolvingRSocket;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public abstract class RSocketPoolElastic<S extends RSocket> implements RSocketPool, PoolOperations {

    Logger logger = LoggerFactory.getLogger(RSocketPoolElastic.class);

    public static final int DEFAULT_MIN_APERTURE = 3;

    protected Consumer<List<ResolvingRSocket>> rSocketSupplierListConsumer;
    protected Consumer<Long> updateConsumer;

    private final Flux<List<ResolvingRSocket>> availableRSocketSuppliers;
    private final AsyncBaseRSocketPool<S> asyncBaseRSocketPool;

    private AtomicReference<Disposable> poolAvailable = new AtomicReference<>();
    private AtomicInteger aperture = new AtomicInteger(DEFAULT_MIN_APERTURE);

    public RSocketPoolElastic(Publisher<? extends Collection<Mono<? extends RSocket>>> rSocketsPublisher) {
        logger.info("Starting elastic RSocket pool.");
        availableRSocketSuppliers = createHotRSocketSuppliersSource();
        poolAvailable.set(availablePoolUpdater(rSocketsPublisher));
        asyncBaseRSocketPool = rSocketPoolParallelConstructor(activeRSocketPool());
    }

    protected void setUpdatingWihParallelPool() {
        asyncBaseRSocketPool.updatingPool().subscribe(i -> update());
    }

    protected abstract S rSocketMapper(RSocket rSocket);

    protected abstract AsyncBaseRSocketPool<S> rSocketPoolParallelConstructor(
            Publisher<? extends Collection<? extends RSocket>> publisher
    );

    private Flux<List<ResolvingRSocket>> createHotRSocketSuppliersSource() {
        return Flux
                .<List<ResolvingRSocket>>create(sink -> rSocketSupplierListConsumer = sink::next)
                .as(this::hotSource);
    }

    private Disposable availablePoolUpdater(
            Publisher<? extends Collection<Mono<? extends RSocket>>> rSocketsPublisher
    ) {
        return Flux
                .from(rSocketsPublisher)
                .distinctUntilChanged()
                .map(c -> (List<Mono<? extends RSocket>>) new ArrayList<>(c))
                .flatMap(list -> Flux.fromIterable(list).map(ResolvingRSocket::new).collectList())
                .as(flux -> poolUpdater(flux, rSocketSupplierListConsumer));
    }

    private Flux<List<S>> activeRSocketPool() {
        return Flux
                .create(sink -> updateConsumer = sink::next)
                .flatMap(i -> snapshot(availableRSocketSuppliers)
                        .flatMapMany(Flux::fromIterable)
                        .take(aperture.get())
                        .collectList())
                .distinctUntilChanged()
                .flatMap(list -> Flux.fromIterable(list).map(this::rSocketMapper).collectList());
    }

    @Override
    public RSocket select() {
        return asyncBaseRSocketPool.select();
    }

    protected void update() {
        updateConsumer.accept(0L);
    }

    @Override
    public Mono<Void> onClose() {
        logger.info("Cleaning RSocket pool.");
        return asyncBaseRSocketPool
                .onClose()
                .then(Mono.fromRunnable(() -> {
                    sourceControl.onComplete();
                    poolAvailable.get().dispose();
                }));
    }
}