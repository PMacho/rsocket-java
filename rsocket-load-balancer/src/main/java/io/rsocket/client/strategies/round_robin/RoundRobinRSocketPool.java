package io.rsocket.client.strategies.round_robin;

import io.rsocket.RSocket;
import io.rsocket.client.new_classes.RSocketPool;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

public class RoundRobinRSocketPool extends RSocketPool<RSocket> {


    @Override
    public void start(Publisher<? extends Collection<RSocket>> rSocketsPublisher) {
        start(rSocketsPublisher, Function.identity(), this::circle);
    }

    private WeightedRSocket weightedRSocket(RSocket rSocket) {
        return new DefaultWeightedRSocket(weightedRSocketPoolStatistics, rSocket);
    }

    public Mono<List<WeightedRSocket>> circle(Collection<WeightedRSocket> weightedRSockets) {
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
