package io.rsocket.client.new_classes;

import io.rsocket.RSocket;
import reactor.core.publisher.Mono;

public interface WeightedRSocket extends RSocket {

    Mono<Double> algorithmicWeight();

}
