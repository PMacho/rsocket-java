package io.rsocket.client.new_classes;

import io.rsocket.RSocket;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Collection;

public interface RSocketPool {

    void clean();

//    RSocketPool create(Publisher<? extends Collection<RSocket>> rSocketsPublisher);

    Mono<WeightedRSocket> select();

}
