package io.rsocket.addons;

import io.rsocket.RSocket;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Extension of BaseRSocketPool that asynchronously selects the next RSocket.
 *
 * @param <S>
 */
public abstract class AsyncBaseRSocketPool<S extends BaseRSocketPool.PooledRSocket> extends BaseRSocketPool<S> {

    Logger logger = LoggerFactory.getLogger(AsyncBaseRSocketPool.class);

    private static final long DEFAULT_REFRESH_DURATION_MS = 5000;

    private final AtomicReference<RSocket> nextRSocket = new AtomicReference<>();

    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduledThreadPoolExecutor = Executors.newSingleThreadScheduledExecutor();

    public AsyncBaseRSocketPool(Publisher<Collection<RSocket>> rSocketsPublisher) {
        super(rSocketsPublisher);

        scheduledThreadPoolExecutor.scheduleAtFixedRate(
                this::update,
                0,
                DEFAULT_REFRESH_DURATION_MS,
                TimeUnit.MILLISECONDS
        );
    }

    @Override
    public void onNext(Collection<RSocket> sockets) {
        super.onNext(sockets);
        update();
    }

    @Override
    public S pooledRSocket(RSocket rSocket) {
        return strategy.rSocketMapper(new AsyncDefaultPooledRSocket(rSocket));
    }

    @Override
    RSocket doSelect() {
        return nextRSocket.get();
    }

    protected void update() {
        executorService.submit(() -> nextRSocket.set(strategy.apply(activeSockets)));
    }

    // todo:
//    @Override
//    public void dispose() {
//        scheduledThreadPoolExecutor.shutdown();
//    }

    class AsyncDefaultPooledRSocket extends DefaultPooledRSocket {

        public AsyncDefaultPooledRSocket(RSocket source) {
            super(source);
        }

        @Override
        protected <T> Mono<T> trackedMono(Supplier<Mono<T>> request) {
            return super.trackedMono(request).doFinally(signalType -> update());
        }

        @Override
        protected <T> Flux<T> trackedFlux(Supplier<Flux<T>> request) {
            return super.trackedFlux(request).doFinally(signalType -> update());
        }

    }

}
