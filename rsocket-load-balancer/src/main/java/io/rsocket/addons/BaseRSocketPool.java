package io.rsocket.addons;

import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.addons.strategies.Strategy;
import io.rsocket.frame.FrameType;

import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

import io.rsocket.util.RSocketProxy;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;

public abstract class BaseRSocketPool<S extends BaseRSocketPool.PooledRSocket> extends ResolvingOperator<Void>
        implements RSocketPool, CoreSubscriber<Collection<RSocket>> {

    final DeferredResolutionRSocket deferredResolutionRSocket = new DeferredResolutionRSocket(this);

    @SuppressWarnings("unchecked")
    final S[] EMPTY = (S[]) new PooledRSocket[0];
    @SuppressWarnings("unchecked")
    final S[] TERMINATED = (S[]) new PooledRSocket[0];

    // todo: define me
    private final Strategy<S> DEFAULT_STRATEGY;
    protected Strategy<S> strategy;
    volatile S[] activeSockets = EMPTY;

    static final AtomicReferenceFieldUpdater<BaseRSocketPool, PooledRSocket[]> ACTIVE_SOCKETS =
            AtomicReferenceFieldUpdater.newUpdater(
                    BaseRSocketPool.class, PooledRSocket[].class, "activeSockets");

    volatile Subscription s;
    static final AtomicReferenceFieldUpdater<BaseRSocketPool, Subscription> S =
            AtomicReferenceFieldUpdater.newUpdater(BaseRSocketPool.class, Subscription.class, "s");

    public BaseRSocketPool(Publisher<Collection<RSocket>> source) {
        strategy(DEFAULT_STRATEGY);
        source.subscribe(this);
    }

    public BaseRSocketPool<S> strategy(Strategy<S> strategy) {
        this.strategy = strategy;
        return this;
    }

    @Override
    protected void doOnDispose() {
        Operators.terminate(S, this);

        RSocket[] activeSockets = ACTIVE_SOCKETS.getAndSet(this, TERMINATED);
        for (RSocket rSocket : activeSockets) {
            rSocket.dispose();
        }
    }

    @Override
    public void onSubscribe(Subscription s) {
        if (Operators.setOnce(S, this, s)) {
            s.request(Long.MAX_VALUE);
        }
    }

    /**
     * This operation should happen rarely relatively compares the number of the {@link #select()}
     * method invocations, therefore it is acceptable to have it algorithmically inefficient. The
     * algorithmic complexity of this method is
     *
     * @param sockets set of newly received unresolved {@link RSocket}s
     */
    @Override
    public void onNext(Collection<RSocket> sockets) {
        if (isDisposed()) {
            return;
        }

        for (; ; ) {

            HashSet<RSocket> set = new HashSet<>(sockets);
            HashSet<S> newSockets = new HashSet<>(sockets.size());

            for (S rSocket : activeSockets) {
                RSocket activeParent = rSocket.parent();

                if (set.contains(activeParent)) {
                    set.remove(activeParent);
                    newSockets.add(rSocket);
                } else {
                    // todo:
//                        rSocket.setPending()
                }
            }

            for (RSocket rSocket : set) {
                newSockets.add(pooledRSocket(rSocket));
            }

            if (ACTIVE_SOCKETS.compareAndSet(this, activeSockets, (PooledRSocket[]) newSockets.toArray())) {
                break;
            }

        }

        if (isPending()) {
            // notifies that upstream is resolved
            complete();
        }
    }

    public S pooledRSocket(RSocket rSocket) {
        return strategy.rSocketMapper(new DefaultPooledRSocket(rSocket));
    }

    @Override
    public void onError(Throwable t) {
        // indicates upstream termination
        S.set(this, Operators.cancelledSubscription());
        // propagates error and terminates the whole pool
        terminate(t);
    }

    @Override
    public void onComplete() {
        // indicates upstream termination
        S.set(this, Operators.cancelledSubscription());
    }

    @Override
    public RSocket select() {
        if (isDisposed()) {
            return this.deferredResolutionRSocket;
        }

        RSocket selected = doSelect();

        if (selected == null) {
            if (this.s == Operators.cancelledSubscription()) {
                terminate(new CancellationException("Pool is exhausted"));
            } else {
                invalidate();
            }
            return this.deferredResolutionRSocket;
        }

        return selected;
    }

    @Nullable
    RSocket doSelect() {
        return strategy.apply(activeSockets);
    }

    static class DeferredResolutionRSocket implements RSocket {

        final BaseRSocketPool<? extends PooledRSocket> parent;

        DeferredResolutionRSocket(BaseRSocketPool<? extends PooledRSocket> parent) {
            this.parent = parent;
        }

        @Override
        public Mono<Void> fireAndForget(Payload payload) {
            return new PooledMonoInner<>(this.parent, payload, FrameType.REQUEST_FNF);
        }

        @Override
        public Mono<Payload> requestResponse(Payload payload) {
            return new PooledMonoInner<>(this.parent, payload, FrameType.REQUEST_RESPONSE);
        }

        @Override
        public Flux<Payload> requestStream(Payload payload) {
            return new PooledFluxInner<>(this.parent, payload, FrameType.REQUEST_STREAM);
        }

        @Override
        public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            return new PooledFluxInner<>(this.parent, payloads, FrameType.REQUEST_STREAM);
        }

        @Override
        public Mono<Void> metadataPush(Payload payload) {
            return new PooledMonoInner<>(this.parent, payload, FrameType.METADATA_PUSH);
        }
    }

    static class PooledMonoInner<T> extends ResolvingOperator.MonoDeferredResolution<T, Void> {

        final BaseRSocketPool<? extends PooledRSocket> parent;
        final Payload payload;
        final FrameType requestType;

        PooledMonoInner(BaseRSocketPool<? extends PooledRSocket> parent, Payload payload, FrameType requestType) {
            super(parent);
            this.parent = parent;
            this.payload = payload;
            this.requestType = requestType;
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public void accept(Void aVoid, Throwable t) {
            if (this.requested == STATE_CANCELLED) {
                return;
            }

            if (t != null) {
                ReferenceCountUtil.safeRelease(this.payload);
                onError(t);
                return;
            }

            BaseRSocketPool parent = this.parent;
            RSocket rSocket = parent.doSelect();
            if (rSocket != null) {
                Mono<?> source;
                switch (this.requestType) {
                    case REQUEST_FNF:
                        source = rSocket.fireAndForget(this.payload);
                        break;
                    case REQUEST_RESPONSE:
                        source = rSocket.requestResponse(this.payload);
                        break;
                    case METADATA_PUSH:
                        source = rSocket.metadataPush(this.payload);
                        break;
                    default:
                        Operators.error(this.actual, new IllegalStateException("Should never happen"));
                        return;
                }

                source.subscribe((CoreSubscriber) this);
            } else {
                parent.add(this);
            }
        }

        public void cancel() {
            long state = REQUESTED.getAndSet(this, STATE_CANCELLED);
            if (state == STATE_CANCELLED) {
                return;
            }

            if (state == STATE_SUBSCRIBED) {
                this.s.cancel();
            } else {
                this.parent.remove(this);
                ReferenceCountUtil.safeRelease(this.payload);
            }
        }
    }

    static class PooledFluxInner<T> extends ResolvingOperator.FluxDeferredResolution<Payload, Void> {

        final BaseRSocketPool<? extends PooledRSocket> parent;
        final T fluxOrPayload;
        final FrameType requestType;

        PooledFluxInner(BaseRSocketPool<? extends PooledRSocket> parent, T fluxOrPayload, FrameType requestType) {
            super(parent);
            this.parent = parent;
            this.fluxOrPayload = fluxOrPayload;
            this.requestType = requestType;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void accept(Void aVoid, Throwable t) {
            if (this.requested == STATE_CANCELLED) {
                return;
            }

            if (t != null) {
                ReferenceCountUtil.safeRelease(this.fluxOrPayload);
                onError(t);
                return;
            }

            BaseRSocketPool<? extends PooledRSocket> parent = this.parent;
            RSocket rSocket = parent.doSelect();
            if (rSocket != null) {
                Flux<? extends Payload> source;
                switch (this.requestType) {
                    case REQUEST_STREAM:
                        source = rSocket.requestStream((Payload) this.fluxOrPayload);
                        break;
                    case REQUEST_CHANNEL:
                        source = rSocket.requestChannel((Flux<Payload>) this.fluxOrPayload);
                        break;
                    default:
                        Operators.error(this.actual, new IllegalStateException("Should never happen"));
                        return;
                }

                source.subscribe(this);
            } else {
                parent.add(this);
            }
        }

        public void cancel() {
            long state = REQUESTED.getAndSet(this, STATE_CANCELLED);
            if (state == STATE_CANCELLED) {
                return;
            }

            if (state == STATE_SUBSCRIBED) {
                this.s.cancel();
            } else {
                this.parent.remove(this);
                ReferenceCountUtil.safeRelease(this.fluxOrPayload);
            }
        }
    }

    /**
     * Specific interface for all RSocket store in {@link RSocketPool}
     */
    public interface PooledRSocket extends RSocket {

        /**
         * Indicates number of active requests
         *
         * @return number of requests in progress
         */
        int activeRequests();

        /**
         * Try to dispose this instance if possible. Otherwise, if there is ongoing requests, mark this
         * as pending for removal and dispose once all the requests are terminated.<br>
         * This operation may be cancelled if {@link #markActive()} is invoked prior this instance has
         * been disposed
         *
         * @return {@code true} if this instance was disposed
         */
        boolean markForRemoval();

        /**
         * Try to restore state of this RSocket to be active after marking as pending removal again.
         *
         * @return {@code true} if marked as active. Otherwise, should be treated as it was disposed.
         */
        boolean markActive();

        /**
         * Allows access to the underlying RSocket.
         *
         * @return the parent RSocket
         */
        RSocket parent();
    }

    public static class DefaultPooledRSocket extends RSocketProxy implements PooledRSocket {

        private AtomicInteger requests = new AtomicInteger(0);
        private AtomicBoolean active = new AtomicBoolean(true);

        public DefaultPooledRSocket(RSocket source) {
            super(source);
        }

        protected <T> Mono<T> trackedMono(Supplier<Mono<T>> request) {
            requests.incrementAndGet();
            return request.get().doFinally(signalType -> requests.decrementAndGet());
        }

        protected <T> Flux<T> trackedFlux(Supplier<Flux<T>> request) {
            requests.incrementAndGet();
            return request.get().doFinally(signalType -> requests.decrementAndGet());
        }

        @Override
        public Mono<Void> fireAndForget(Payload payload) {
            return trackedMono(() -> source.fireAndForget(payload));
        }

        @Override
        public Mono<Payload> requestResponse(Payload payload) {
            return trackedMono(() -> source.requestResponse(payload));
        }

        @Override
        public Flux<Payload> requestStream(Payload payload) {
            return trackedFlux(() -> source.requestStream(payload));
        }

        @Override
        public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            return trackedFlux(() -> source.requestChannel(payloads));
        }

        @Override
        public Mono<Void> metadataPush(Payload payload) {
            return trackedMono(() -> source.metadataPush(payload));
        }

        @Override
        public int activeRequests() {
            return requests.get();
        }

        @Override
        public boolean markForRemoval() {
            return active.compareAndSet(true, false);
        }

        @Override
        public boolean markActive() {
            return active.compareAndSet(false, true);
        }

        @Override
        public RSocket parent() {
            return this.source;
        }
    }

}
