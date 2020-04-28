package io.rsocket.client.new_classes;

import io.rsocket.RSocket;

public interface WeightedRSocket extends RSocket {

    double algorithmicWeight();

}
