package io.rsocket.client.strategies.round_robin;

import io.rsocket.RSocket;
import io.rsocket.client.new_classes.RSocketPoolStatic;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

public class RoundRobinRSocketPool extends RSocketPoolStatic<RSocket> {

    @Override
    public void start(Publisher<? extends Collection<RSocket>> rSocketsPublisher) {
        start(rSocketsPublisher, Function.identity(), this::circle);
    }

    public Mono<List<RSocket>> circle(List<RSocket> rSockets) {
        LinkedList<RSocket> linkedList = new LinkedList<>(rSockets);

        linkedList.add(linkedList.poll());
        return Mono.just(linkedList);
    }

}
