package io.rsocket.addons.strategies;

import io.rsocket.addons.BaseRSocketPool;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class RoundRobinStrategy implements Strategy<BaseRSocketPool.PooledRSocket> {

    volatile int nextIndex;

    static final AtomicIntegerFieldUpdater<RoundRobinStrategy> NEXT_INDEX =
            AtomicIntegerFieldUpdater.newUpdater(RoundRobinStrategy.class, "nextIndex");

    @Override
    public BaseRSocketPool.PooledRSocket rSocketMapper(BaseRSocketPool.PooledRSocket pooledRSocket) {
        return pooledRSocket;
    }

    @Override
    public BaseRSocketPool.PooledRSocket apply(BaseRSocketPool.PooledRSocket[] pooledRSockets) {
        int indexToUse = Math.abs(NEXT_INDEX.getAndIncrement(this) % pooledRSockets.length);
        return pooledRSockets[indexToUse];
    }

}
