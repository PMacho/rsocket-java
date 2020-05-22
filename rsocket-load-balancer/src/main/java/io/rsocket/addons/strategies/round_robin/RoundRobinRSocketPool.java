package io.rsocket.addons.strategies.round_robin;

import io.rsocket.RSocket;
import io.rsocket.addons.pools.RSocketPoolParallel;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class RoundRobinRSocketPool extends RSocketPoolParallel<RSocket> {

    public RoundRobinRSocketPool(Publisher<? extends Collection<RSocket>> rSocketsPublisher) {
        super(rSocketsPublisher);
    }

    @Override
    protected RSocket rSocketMapper(RSocket rSocket) {
        return rSocket;
    }

    @Override
    protected Mono<List<RSocket>> orderRSockets(List<RSocket> rSockets) {
        final LinkedList<RSocket> linkedList;

        if (!(rSockets instanceof LinkedList)) {
            linkedList = new LinkedList<>(rSockets);
        } else {
            linkedList = (LinkedList<RSocket>) rSockets;
        }

        linkedList.add(linkedList.poll());
        return Mono.just(linkedList);
    }

}
