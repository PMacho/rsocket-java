package io.rsocket.client.strategies.weighted;


import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.client.TimeoutException;
import io.rsocket.client.TransportException;
import io.rsocket.util.RSocketProxy;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.channels.ClosedChannelException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

public class DefaultWeightedRSocket extends RSocketProxy implements WeightedRSocket {

    Logger logger = LoggerFactory.getLogger(DefaultWeightedRSocket.class);

    private static final double DEFAULT_EXPONENTIAL_FACTOR = 4.0;

    private volatile double availability;

    private final WeightedRSocketPoolStatistics weightedRSocketPoolStatistics;
    private final ConcurrentSubscriptionTracker concurrentSubscriptionTracker;

    private final double exponentialFactor;

    DefaultWeightedRSocket(
            WeightedRSocketPoolStatistics weightedRSocketPoolStatistics,
            RSocket rSocket,
            Integer inactivityFactor
    ) {
        super(rSocket);

        final Consumer<Double> updateQuantiles = weightedRSocketPoolStatistics::updateQuantiles;
        this.concurrentSubscriptionTracker = Optional
                .ofNullable(inactivityFactor)
                .map(factor -> new ConcurrentSubscriptionTracker(updateQuantiles, factor))
                .orElseGet(() -> new ConcurrentSubscriptionTracker(updateQuantiles));
        this.weightedRSocketPoolStatistics = weightedRSocketPoolStatistics;

        availability = 1.0;
        exponentialFactor = DEFAULT_EXPONENTIAL_FACTOR;

        logger.debug("Creating WeightedRSocket {} for RSocket {}", DefaultWeightedRSocket.this, rSocket);
    }

    DefaultWeightedRSocket(
            WeightedRSocketPoolStatistics weightedRSocketPoolStatistics,
            RSocket rSocket
    ) {
        this(weightedRSocketPoolStatistics, rSocket, null);
    }

//    WeightedSocket(
//            RSocketSupplier factory,
//            Quantile lowerQuantile,
//            Quantile higherQuantile,
//            int inactivityFactor
//    ) {
//        this.rSocketMono = MonoProcessor.create();
//        this.lowerQuantile = lowerQuantile;
//        this.higherQuantile = higherQuantile;
//        this.inactivityFactor = inactivityFactor;
//        long now = Clock.now();
//        this.stamp = now;
//        this.stamp0 = now;
//        this.duration = 0L;
//        this.pending = 0;
//        this.median = new Median();
//        this.interArrivalTime = new Ewma(1, TimeUnit.MINUTES, DEFAULT_INITIAL_INTER_ARRIVAL_TIME);
//        this.pendingStreams = new AtomicLong();
//
//        logger.debug("Creating WeightedSocket {} from factory {}", WeightedSocket.this, factory);
//
//        WeightedSocket.this
//                .onClose()
//                .doFinally(
//                        s -> {
//                            pool.accept(factory);
//                            activeSockets.remove(WeightedSocket.this);
//                            logger.debug(
//                                    "Removed {} from factory {} from activeSockets", WeightedSocket.this, factory);
//                        })
//                .subscribe();
//
//        factory
//                .get()
//                .retryBackoff(weightedSocketRetries, weightedSocketBackOff, weightedSocketMaxBackOff)
//                .doOnError(
//                        throwable -> {
//                            logger.error(
//                                    "error while connecting {} from factory {}",
//                                    WeightedSocket.this,
//                                    factory,
//                                    throwable);
//                            WeightedSocket.this.dispose();
//                        })
//                .subscribe(
//                        rSocket -> {
//                            // When RSocket is closed, close the WeightedSocket
//                            rSocket
//                                    .onClose()
//                                    .doFinally(
//                                            signalType -> {
//                                                logger.info(
//                                                        "RSocket {} from factory {} closed", WeightedSocket.this, factory);
//                                                WeightedSocket.this.dispose();
//                                            })
//                                    .subscribe();
//
//                            // When the factory is closed, close the RSocket
//                            factory
//                                    .onClose()
//                                    .doFinally(
//                                            signalType -> {
//                                                logger.info("Factory {} closed", factory);
//                                                rSocket.dispose();
//                                            })
//                                    .subscribe();
//
//                            // When the WeightedSocket is closed, close the RSocket
//                            WeightedSocket.this
//                                    .onClose()
//                                    .doFinally(
//                                            signalType -> {
//                                                logger.info(
//                                                        "WeightedSocket {} from factory {} closed",
//                                                        WeightedSocket.this,
//                                                        factory);
//                                                rSocket.dispose();
//                                            })
//                                    .subscribe();
//
//                /*synchronized (LoadBalancedRSocketMono.this) {
//                  if (activeSockets.size() >= targetAperture) {
//                    quickSlowestRS();
//                    pendingSockets -= 1;
//                  }
//                }*/
//                            rSocketMono.onNext(rSocket);
//                            availability = 1.0;
//                            if (!WeightedSocket.this
//                                    .isDisposed()) { // May be already disposed because of retryBackoff delay
//                                activeSockets.add(WeightedSocket.this);
//                                logger.debug(
//                                        "Added WeightedSocket {} from factory {} to activeSockets",
//                                        WeightedSocket.this,
//                                        factory);
//                            }
//                        });
//    }

    @Override
    public Mono<Double> algorithmicWeight() {
        if (availability() == 0.0) {
            return Mono.just(0.0);
        }

        return weightedRSocketPoolStatistics
                .getQuantiles()
                .flatMap(
                        quantiles -> concurrentSubscriptionTracker.consume(
                                tracker -> WeightingStatisticsUtil.algorithmicWeight(
                                        quantiles.getLowerQuantile(),
                                        quantiles.getHigherQuantile(),
                                        tracker.predictedLatency(),
                                        tracker.pending(),
                                        exponentialFactor,
                                        // fixme: ?
                                        availability
                                )
                        )
                );
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        return Mono.from(
                subscriber -> source
                        .requestResponse(payload)
                        .subscribe(new LatencySubscriber<>(subscriber, this))
        );
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
        return Flux.from(
                subscriber -> source
                        .requestStream(payload)
                        .subscribe(new CountingSubscriber<>(subscriber, this))
        );
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        return Mono.from(
                subscriber -> source
                        .fireAndForget(payload)
                        .subscribe(new CountingSubscriber<>(subscriber, this))
        );
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
        return Mono.from(
                subscriber -> source
                        .metadataPush(payload)
                        .subscribe(new CountingSubscriber<>(subscriber, this))
        );
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return Flux.from(
                subscriber -> source
                        .requestChannel(payloads)
                        .subscribe(new CountingSubscriber<>(subscriber, this))
        );
    }

    @Override
    public double availability() {
        return availability;
    }

    @Override
    public String toString() {
        return "WeightedSocket("
//                + "median="
//                + median.estimation()
//                + " quantile-low="
//                + doWithQuantiles.get().getLowerQuantile()
//                + " quantile-high="
//                + doWithQuantiles.get().getHigherQuantile()
//                + " inter-arrival="
//                + interArrivalTime.value()
//                + " duration/pending="
//                + (pending == 0 ? 0 : (double) duration / pending)
//                + " pending="
//                + pending
                + " availability= "
                + availability()
                + ")->";
    }

    /**
     * Subscriber wrapper used for request/response interaction model, measure and collect latency
     * information.
     */
    private class LatencySubscriber<U> implements Subscriber<U> {
        private final Subscriber<U> child;
        private final WeightedRSocket socket;
        private final UUID id;

        LatencySubscriber(Subscriber<U> child, WeightedRSocket socket) {
            this.child = child;
            this.socket = socket;
            this.id = UUID.randomUUID();
        }

        @Override
        public void onSubscribe(Subscription s) {
            concurrentSubscriptionTracker.addLatencySubscriber(id);
            child.onSubscribe(
                    new Subscription() {
                        @Override
                        public void request(long n) {
                            s.request(n);
                        }

                        @Override
                        public void cancel() {
                            s.cancel();
                            concurrentSubscriptionTracker.removeLatencySubscriber(id);
                        }
                    });
        }

        @Override
        public void onNext(U u) {
            child.onNext(u);
        }

        @Override
        public void onError(Throwable t) {
            child.onError(t);
            if (t instanceof TransportException || t instanceof ClosedChannelException) {
                socket.dispose();
                concurrentSubscriptionTracker.removeLatencySubscriber(id);
            } else if (t instanceof TimeoutException) {
                concurrentSubscriptionTracker.removeLatencySubscriberAndUpdateQuantiles(id);
            }
        }

        @Override
        public void onComplete() {
            child.onComplete();
            concurrentSubscriptionTracker.removeLatencySubscriberAndUpdateQuantiles(id);
        }

    }

    /**
     * Subscriber wrapper used for stream like interaction model, it only counts the number of
     * active streams
     */
    private class CountingSubscriber<U> implements Subscriber<U> {
        private final Subscriber<U> child;
        private final WeightedRSocket socket;
        private final UUID id = UUID.randomUUID();

        CountingSubscriber(Subscriber<U> child, WeightedRSocket socket) {
            this.child = child;
            this.socket = socket;
        }

        @Override
        public void onSubscribe(Subscription s) {
            child.onSubscribe(s);
            concurrentSubscriptionTracker.addCountingSubscriber(id);
        }

        @Override
        public void onNext(U u) {
            child.onNext(u);
        }

        @Override
        public void onError(Throwable t) {
            child.onError(t);
            concurrentSubscriptionTracker.removeCountingSubscriber(id);
            if (t instanceof TransportException || t instanceof ClosedChannelException) {
                logger.debug("Disposing {} from activeSockets because of error {}", socket, t);
                socket.dispose();
            }
        }

        @Override
        public void onComplete() {
            child.onComplete();
            concurrentSubscriptionTracker.removeCountingSubscriber(id);
        }

    }

}
