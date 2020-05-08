package io.rsocket.client.new_classes;

import io.rsocket.RSocket;
import io.rsocket.client.filter.RSocketSupplier;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;
import java.util.function.Supplier;

public class WeightedRSocketPool {

    private final ConcurrentOperationsWrapper<WeightedRSocketPoolStatistics> loadBalancingStatisticsOperations;
    private ConcurrentSkipListSet<RSocketSupplier> rSocketSuppliers;

    private ConnectableFlux<RSocket> weightedRSocketFlux;

    public WeightedRSocketPool(
            Collection<RSocketSupplier> rSocketSuppliers
    ) {
        this.loadBalancingStatisticsOperations = new ConcurrentOperationsWrapper<>(new WeightedRSocketPoolStatistics());
        updateSuppliers(rSocketSuppliers);
    }

    public void updateSuppliers(Collection<RSocketSupplier> rSocketSuppliers) {
        this.rSocketSuppliers.addAll(rSocketSuppliers);
    }

    private final ConcurrentSkipListSet<WeightedRSocket> activeSockets = new ConcurrentSkipListSet<>(
            Comparator.comparingDouble(WeightedRSocket::algorithmicWeight)
    );

    private Mono<WeightedRSocket> weightedRSocket(RSocketSupplier rSocketSupplier) {
        return rSocketSupplier
                .get()
                .map(rSocket -> new DefaultWeightedRSocket(loadBalancingStatisticsOperations, rSocket));
    }

    private void updateQuantiles(double rtt) {
        loadBalancingStatisticsOperations.write(
                weightedRSocketPoolStatistics -> weightedRSocketPoolStatistics.updateQuantiles(rtt)
        );
    }

    private Supplier<WeightedRSocketPoolStatistics.Quantiles> lala() {
        return () -> loadBalancingStatisticsOperations.read(WeightedRSocketPoolStatistics::getQuantiles);
    }

    private <S> S withQuantiles(Function<WeightedRSocketPoolStatistics.Quantiles, S> quantilesConsumer) {
        return loadBalancingStatisticsOperations.read(
                weightedRSocketPoolStatistics -> quantilesConsumer.apply(weightedRSocketPoolStatistics.getQuantiles())
        );
    }

    private class Quantiles {
        double higher;
        double lower;
    }

    private Mono<RSocket> select() {

        RSocketProxy

    }
}
