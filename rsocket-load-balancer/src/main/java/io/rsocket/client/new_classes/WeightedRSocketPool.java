package io.rsocket.client.new_classes;

import io.netty.util.concurrent.ImmediateExecutor;
import io.rsocket.RSocket;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class WeightedRSocketPool implements RSocketPool {

    Logger logger = LoggerFactory.getLogger(WeightedRSocketPool.class);

    private Consumer<List<WeightedRSocket>> weightedRSocketListConsumer;
    private Consumer<Long> updateConsumer;

    private final Flux<List<WeightedRSocket>> hotRSocketsSource;

    private final WeightedRSocketPoolStatistics weightedRSocketPoolStatistics = new WeightedRSocketPoolStatistics();

    private final AtomicReference<Disposable> state;
    private final DirectProcessor<Void> control = DirectProcessor.create();

    public WeightedRSocketPool(Publisher<? extends Collection<RSocket>> rSocketsPublisher) {
        hotRSocketsSource = createHotRSocketsSource();
        state = new AtomicReference<>(start(rSocketsPublisher));
    }

    private Flux<List<WeightedRSocket>> createHotRSocketsSource() {
        return Flux
                .<List<WeightedRSocket>>create(sink -> weightedRSocketListConsumer = sink::next)
                .share()
                .cache(1)
                .takeUntilOther(control);
    }

    public void clean() {
        control.onComplete();
        state.get().dispose();
    }

    public Disposable start(Publisher<? extends Collection<RSocket>> rSocketsPublisher) {
        return Flux
                .from(rSocketsPublisher)
                .distinctUntilChanged()
                .flatMap(this::weightedRSockets)
                .switchMap(weightedRSockets -> Flux
                        .merge(
                                Flux.interval(Duration.ZERO, Duration.ofMillis(2000)),
                                Flux.create(sink -> updateConsumer = sink::next)
                        )
                        .flatMap(i -> sortWeightedRSockets(weightedRSockets))
                        .doOnNext(rSockets -> weightedRSocketListConsumer.accept(rSockets))
                )
                .onErrorContinue((throwable, o) -> logger.error("Received error signal for " + o, throwable))
                .subscribe();
    }

    public void updateRSocketStatistics() {
        updateConsumer.accept(0L);
    }

    @Override
    public Mono<WeightedRSocket> select() {
        return getFastest();
    }

    public Mono<WeightedRSocket> getFastest() {
        return Flux
                .from(hotRSocketsSource)
                .next()
                .map(list -> list.get(0))
                .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofMillis(10)));
    }

    private Mono<List<WeightedRSocket>> weightedRSockets(Collection<RSocket> rSockets) {
        return Flux.fromIterable(rSockets).map(this::weightedRSocket).collectList();
    }

    private WeightedRSocket weightedRSocket(RSocket rSocket) {
        return new DefaultWeightedRSocket(weightedRSocketPoolStatistics, rSocket);
    }

//    public void updateSuppliers(Collection<RSocketSupplier> rSocketSuppliers) {
//        this.rSocketSuppliers.addAll(rSocketSuppliers);
//    }
//
//    public void updateWeightedRSockets() {
////        CopyOnWriteArrayList
//        Flux
//                .fromIterable(weightedRSockets)
//                .flatMap(weightedRSocket -> weightedRSocket
//                        .algorithmicWeight()
//                        .map(weight -> new RSocketWithWeight(weightedRSocket, weight))
//                )
//                .collectSortedList(Comparator.comparingDouble(RSocketWithWeight::getWeight));
//    }

    public Mono<List<WeightedRSocket>> sortWeightedRSockets(Collection<WeightedRSocket> weightedRSockets) {
//        CopyOnWriteArrayList
        return Flux
                .fromIterable(weightedRSockets)
                .flatMap(weightedRSocket -> weightedRSocket
                        .algorithmicWeight()
                        .map(weight -> new RSocketWithWeight(weightedRSocket, weight))
                )
                .sort(Comparator.comparingDouble(RSocketWithWeight::getWeight))
                .map(RSocketWithWeight::getWeightedRSocket)
                .collectList();
    }

//    private final ConcurrentSkipListSet<WeightedRSocket> activeSockets = new ConcurrentSkipListSet<>(
//            Comparator.comparingDouble(WeightedRSocket::algorithmicWeight)
//    );
//
//    private Mono<WeightedRSocket> weightedRSocket(RSocketSupplier rSocketSupplier) {
//        return rSocketSupplier
//                .get()
//                .map(rSocket -> new DefaultWeightedRSocket(weightedRSocketPoolStatistics, rSocket));
//    }

//    private void updateQuantiles(double rtt) {
//        loadBalancingStatisticsOperations.write(
//                weightedRSocketPoolStatistics -> weightedRSocketPoolStatistics.updateQuantiles(rtt)
//        );
//    }

//    private Supplier<WeightedRSocketPoolStatistics.QuantilesWrapper> lala() {
//        return () -> loadBalancingStatisticsOperations.read(WeightedRSocketPoolStatistics::getQuantiles);
//    }

//    private <S> S withQuantiles(Function<WeightedRSocketPoolStatistics.QuantilesWrapper, S> quantilesConsumer) {
//        return loadBalancingStatisticsOperations.read(
//                weightedRSocketPoolStatistics -> quantilesConsumer.apply(weightedRSocketPoolStatistics.getQuantiles())
//        );
//    }

//    private class Quantiles {
//        double higher;
//        double lower;
//    }
//
//    private Mono<RSocket> select() {
//
//        RSocketProxy
//
//    }

    private static class RSocketWithWeight {
        private final WeightedRSocket weightedRSocket;
        private final double weight;

        public RSocketWithWeight(WeightedRSocket weightedRSocket, double weight) {
            this.weightedRSocket = weightedRSocket;
            this.weight = weight;
        }

        public WeightedRSocket getWeightedRSocket() {
            return weightedRSocket;
        }

        public double getWeight() {
            return weight;
        }
    }
}
