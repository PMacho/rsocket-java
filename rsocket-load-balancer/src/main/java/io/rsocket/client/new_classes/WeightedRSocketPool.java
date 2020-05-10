package io.rsocket.client.new_classes;

import io.rsocket.RSocket;
import io.rsocket.client.filter.RSocketSupplier;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class WeightedRSocketPool extends LockedOperations {
    public Consumer<List<WeightedRSocket>> weightedRSocketListConsumer;

    public Flux<List<WeightedRSocket>> createNumberSequence() {
        return Flux
                .create(sink -> WeightedRSocketPool.this.weightedRSocketListConsumer = sink::next)
                .share();
    }
    private ConcurrentSkipListSet<RSocketSupplier> rSocketSuppliers;
    private ConcurrentSkipListSet<WeightedRSocket> weightedRSockets;

    private final WeightedRSocketPoolStatistics weightedRSocketPoolStatistics = new WeightedRSocketPoolStatistics();


//    private final ConcurrentOperationsWrapper<WeightedRSocketPoolStatistics> loadBalancingStatisticsOperations;

    private ConnectableFlux<RSocket> weightedRSocketFlux;

    public WeightedRSocketPool(
            Collection<RSocketSupplier> rSocketSuppliers
    ) {
        rSocketSuppliers = new ConcurrentSkipListSet<>(Comparator.comparingDouble(RSocketSupplier::availability));
        weightedRSockets = new ConcurrentSkipListSet<>(Comparator.comparingDouble(WeightedRSocket::algorithmicWeight));
        updateSuppliers(rSocketSuppliers);
    }

    public void updateSuppliers(Collection<RSocketSupplier> rSocketSuppliers) {
        this.rSocketSuppliers.addAll(rSocketSuppliers);
    }

    public void updateWeightedRSockets() {
        CopyOnWriteArrayList
        Flux
                .fromIterable(weightedRSockets)
                .flatMap(weightedRSocket -> weightedRSocket
                        .algorithmicWeight()
                        .map(weight -> new RSocketWithWeight(weightedRSocket, weight))
                )
                .collectSortedList(Comparator.comparingDouble(RSocketWithWeight::getWeight))
    }

    private final ConcurrentSkipListSet<WeightedRSocket> activeSockets = new ConcurrentSkipListSet<>(
            Comparator.comparingDouble(WeightedRSocket::algorithmicWeight)
    );

    private Mono<WeightedRSocket> weightedRSocket(RSocketSupplier rSocketSupplier) {
        return rSocketSupplier
                .get()
                .map(rSocket -> new DefaultWeightedRSocket(weightedRSocketPoolStatistics, rSocket));
    }

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
