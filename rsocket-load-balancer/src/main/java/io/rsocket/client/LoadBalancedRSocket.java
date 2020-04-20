package io.rsocket.client;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListSet;

public class LoadBalancedRSocket extends AbstractRSocket {

    private final ConcurrentSkipListSet<RSocket> activeSockets = new ConcurrentSkipListSet<>(
            Comparator.comparingDouble()
    )

    private Mono<RSocket> select() {

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
    // fixme: Am I this really correct?
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
