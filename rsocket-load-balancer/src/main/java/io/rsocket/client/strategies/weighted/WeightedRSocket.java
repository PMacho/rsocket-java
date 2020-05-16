package io.rsocket.client.strategies.weighted;

import io.rsocket.RSocket;
import reactor.core.publisher.Mono;

public interface WeightedRSocket extends RSocket {

    Mono<Double> algorithmicWeight();

}
