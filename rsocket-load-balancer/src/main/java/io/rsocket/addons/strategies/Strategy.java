package io.rsocket.addons.strategies;

import io.rsocket.addons.BaseRSocketPool;

import java.util.function.Function;

/**
 * Strategy for RSocketPool to select the next RSocket.
 */
public interface Strategy<S extends BaseRSocketPool.PooledRSocket> extends Function<S[], S> {

    S rSocketMapper(BaseRSocketPool.PooledRSocket pooledRSocket);
}
