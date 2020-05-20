package io.rsocket.client.new_classes;

import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class RSocketPoolElastic<S extends RSocket> implements RSocketPool<S> {

    Logger logger = LoggerFactory.getLogger(RSocketPoolElastic.class);

    public static final int DEFAULT_MIN_APERTURE = 3;

    protected Consumer<List<Supplier<RSocket>>> rSocketSupplierListConsumer;
    protected Consumer<Long> updateConsumer;

    private final Flux<List<Supplier<RSocket>>> availableRSocketSuppliers;
    private final RSocketPoolStatic<S> rSocketPoolStatic;

    private AtomicReference<Disposable> poolAvailable = new AtomicReference<>();
    private AtomicInteger aperture = new AtomicInteger(DEFAULT_MIN_APERTURE);


    public RSocketPoolElastic(RSocketPoolStatic<S> rSocketPoolStatic) {
        this.rSocketPoolStatic = rSocketPoolStatic;
        availableRSocketSuppliers = createHotRSocketSuppliersSource();
    }

    private Flux<List<Supplier<RSocket>>> createHotRSocketSuppliersSource() {
        return Flux
                .<List<Supplier<RSocket>>>create(sink -> rSocketSupplierListConsumer = sink::next)
                .as(this::hotSource);
    }

    protected void start(
            Publisher<? extends Collection<Supplier<RSocket>>> rSocketsPublisher,
            Function<Supplier<RSocket>, S> rSocketMapper
    ) {
        logger.info("Starting elastic RSocket pool.");
        poolAvailable.set(availablePoolUpdater(rSocketsPublisher));
        activeRSocketPool(rSocketMapper);
    }

    private Disposable availablePoolUpdater(Publisher<? extends Collection<Supplier<RSocket>>> rSocketsPublisher) {
        return Flux
                .from(rSocketsPublisher)
                .distinctUntilChanged()
                .map(c -> (List<Supplier<RSocket>>) new ArrayList<>(c))
                .as(flux -> poolUpdater(flux, rSocketSupplierListConsumer));
    }

    private void activeRSocketPool(Function<Supplier<RSocket>, S> rSocketMapper) {
        rSocketPoolStatic.start(
                Flux
                        .create(sink -> updateConsumer = sink::next)
                        .flatMap(i -> snapshot(availableRSocketSuppliers)
                                        .flatMapMany(Flux::fromIterable)
                                        .take(aperture.get())
                                        .collectList()
                        )
                        .distinctUntilChanged()
                        .flatMap(list -> Flux.fromIterable(list).map(rSocketMapper).collectList())
        );
    }

    @Override
    public Mono<S> select() {
        return rSocketPoolStatic.select();
    }

    @Override
    public Flux<S> selectAll() {
        return rSocketPoolStatic.selectAll();
    }

    @Override
    public void update() {
        updateConsumer.accept(0L);
    }

    @Override
    public Mono<Void> clean() {
        logger.info("Cleaning RSocket pool.");
        return rSocketPoolStatic
                .clean()
                .then(Mono.fromRunnable(() -> poolAvailable.get().dispose()));
    }

}
