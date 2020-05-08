package io.rsocket.client.new_classes;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.client.filter.RSocketSupplier;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListSet;

public class LoadBalancedRSocket extends AbstractRSocket {

    private final ConcurrentOperationsWrapper<WeightedRSocketPoolStatistics> loadBalancingStatisticsOperations;

    private ConnectableFlux<RSocket> weightedRSocketFlux;

    public LoadBalancedRSocket(
            Publisher<? extends Collection<RSocketSupplier>> rSocketSuppliers
    ) {
        this.loadBalancingStatisticsOperations = new ConcurrentOperationsWrapper<>(new WeightedRSocketPoolStatistics());
    }

    private final ConcurrentSkipListSet<WeightedRSocket> activeSockets = new ConcurrentSkipListSet<>(
            Comparator.comparingDouble(WeightedRSocket::algorithmicWeight)
    );

    private Mono<RSocket> select() {

        RSocketProxy

    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        return select().flatMap(rSocket -> rSocket.fireAndForget(payload));
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        return select().flatMap(rSocket -> rSocket.requestResponse(payload));
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
        return select().flatMapMany(rSocket -> rSocket.requestStream(payload));
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return select().flatMapMany(rSocket -> rSocket.requestChannel(payloads));
    }

    @Override
    // fixme: Am I really correct?
    public Mono<Void> metadataPush(Payload payload) {
        return Mono
                .when(
                        () -> activeSockets
                                .stream()
                                .<Publisher<?>>map(rSocket -> rSocket.metadataPush(payload))
                                .iterator()
                )
                .then();
    }

    @Override
    public void dispose() {
        synchronized (this) {
            activeSockets.forEach(Disposable::dispose);
            activeSockets.clear();
//            activeSockets.forEach(LoadBalancedRSocketMono.WeightedSocket::dispose);
//            activeSockets.clear();
            super.dispose();
        }
    }


}
