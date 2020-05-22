package io.rsocket.addons.strategies.parallel.weighted;

import io.rsocket.RSocket;
import reactor.core.publisher.Mono;

public interface WeightedRSocket extends RSocket {

  Mono<Double> algorithmicWeight();
}
