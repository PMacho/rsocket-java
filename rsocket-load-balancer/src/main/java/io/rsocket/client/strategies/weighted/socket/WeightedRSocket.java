package io.rsocket.client.strategies.weighted.socket;

import io.rsocket.RSocket;
import reactor.core.publisher.Mono;

public interface WeightedRSocket extends RSocket {

    Mono<Double> algorithmicWeight();

}
