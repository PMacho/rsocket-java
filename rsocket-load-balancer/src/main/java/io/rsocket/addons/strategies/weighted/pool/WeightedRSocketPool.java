package io.rsocket.addons.strategies.weighted.pool;

import io.rsocket.RSocket;
import io.rsocket.addons.pools.RSocketPoolStatic;
import io.rsocket.addons.strategies.weighted.socket.DefaultWeightedRSocket;
import io.rsocket.addons.strategies.weighted.socket.WeightedRSocket;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;

public class WeightedRSocketPool extends RSocketPoolStatic<WeightedRSocket> {

    private final WeightedRSocketPoolStatistics weightedRSocketPoolStatistics = new WeightedRSocketPoolStatistics();

    public WeightedRSocketPool() {
        super();
    }

    @Override
    public void start(Publisher<? extends Collection<RSocket>> rSocketsPublisher) {
        start(rSocketsPublisher, this::weightedRSocket, this::sortWeightedRSockets);
    }

    private WeightedRSocket weightedRSocket(RSocket rSocket) {
        return new DefaultWeightedRSocket(weightedRSocketPoolStatistics, rSocket);
    }

    public Mono<List<WeightedRSocket>> sortWeightedRSockets(List<WeightedRSocket> weightedRSockets) {
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
