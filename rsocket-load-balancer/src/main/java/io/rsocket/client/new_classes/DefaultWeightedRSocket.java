package io.rsocket.client.new_classes;


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

    //    private static final long DEFAULT_INITIAL_INTER_ARRIVAL_TIME =
//            Clock.unit().convert(1L, TimeUnit.SECONDS);
    private static final double DEFAULT_EXPONENTIAL_FACTOR = 4.0;

    private volatile double availability = 0.0;
    private final double exponentialFactor;

    private final Consumer<Double> updateQuantiles;
    private final WeightedRSocketPoolStatistics weightedRSocketPoolStatistics;
    private final ConcurrentSubscriptionTracker concurrentSubscriptionTracker;


    //    private final ConcurrentOperations<WeightingStatistics> weightingStatisticsOperations;
//    private final WeightedRSocketPoolStatistics weightedRSocketPoolStatistics;

    DefaultWeightedRSocket(
            WeightedRSocketPoolStatistics weightedRSocketPoolStatistics,
            RSocket rSocket,
            Integer inactivityFactor
    ) {
        super(rSocket);

//        this.weightingStatistics = Optional
//                .ofNullable(inactivityFactor)
//                .map(WeightingStatistics::new)
//                .orElseGet(WeightingStatistics::new);
        this.updateQuantiles = weightedRSocketPoolStatistics::updateQuantiles;
        this.concurrentSubscriptionTracker = Optional
                .ofNullable(inactivityFactor)
                .map(factor -> new ConcurrentSubscriptionTracker(updateQuantiles, factor))
                .orElseGet(() -> new ConcurrentSubscriptionTracker(updateQuantiles));
        this.weightedRSocketPoolStatistics = weightedRSocketPoolStatistics;
//        this.weightingStatisticsOperations = new ConcurrentOperations<>(weightingStatistics);

//                .write(
//                weightedRSocketPoolStatistics -> weightedRSocketPoolStatistics.updateQuantiles(rtt)
//        );
//        this.weightedRSocketPoolStatisticsOperations = weightedRSocketPoolStatisticsOperations;

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

    private double algorithmicWeight(
            final double lowerQuantile,
            final double higherQuantile,
            final int pending,
            final double predictedLatency

//            WeightedRSocketPoolStatistics.Quantiles quantiles,
//            WeightingStatistics weightingStatistics
    ) {
        // fixme: What is this good for? Should this really be need?
        // ensure higherQuantile > lowerQuantile + .1%
        final double high = Math.max(higherQuantile, lowerQuantile * 1.001);
        final double bandWidth = Math.max(high - lowerQuantile, 1);

//        final int pending = weightingStatistics.getPending();
        double latency = predictedLatency;

        if (latency < lowerQuantile) {
            double alpha = (lowerQuantile - latency) / bandWidth;
            double bonusFactor = Math.pow(1 + alpha, exponentialFactor);
            latency /= bonusFactor;
        } else if (latency > high) {
            double alpha = (latency - high) / bandWidth;
            double penaltyFactor = Math.pow(1 + alpha, exponentialFactor);
            latency *= penaltyFactor;
        }

        return source.availability() * 1.0 / (1.0 + latency * (pending + 1));
    }

    private <U> LatencySubscriber<U> latencySubscriber(Subscriber<U> child, WeightedRSocket socket) {
        return new LatencySubscriber<>(
                child,
                socket
//                rtt -> {
//                    // fixme: make atomic
//                    concurrentPerformanceTracker.updateMedian(rtt);
////                    weightingStatisticsOperations.write(ws -> ws.updateMedian(rtt));
//                    updateQuantiles.accept(rtt);
//                }
        );
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        return Mono.from(
                subscriber -> source
                        .requestResponse(payload)
                        .subscribe(latencySubscriber(subscriber, this))
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

//    private long incr() {
//        long now = now();
//        weightingStatisticsOperations.write(weightingStatistics -> weightingStatistics.incr(now));
//        return now;
//    }

//    private long decr(long start) {
//        long now = now();
//        weightingStatisticsOperations.write(weightingStatistics -> weightingStatistics.decr(start, now));
//        return now;
//    }

//    private long now() {
//        return Clock.now();
//    }

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
        //        private final AtomicBoolean done;
//        private long start;
        private final UUID id;

        LatencySubscriber(Subscriber<U> child, WeightedRSocket socket) {
            this.child = child;
            this.socket = socket;
//            this.done = new AtomicBoolean(false);
            this.id = UUID.randomUUID();
        }

        @Override
        public void onSubscribe(Subscription s) {
            concurrentSubscriptionTracker.addLatencySubscriber(id);
//            start = incr();
            child.onSubscribe(
                    new Subscription() {
                        @Override
                        public void request(long n) {
                            s.request(n);
                        }

                        @Override
                        public void cancel() {
//                            if (done.compareAndSet(false, true)) {
                            s.cancel();
                            concurrentSubscriptionTracker.removeLatencySubscriber(id);
//                                decr(start);
//                            }
                        }
                    });
        }

        @Override
        public void onNext(U u) {
            child.onNext(u);
        }

        @Override
        public void onError(Throwable t) {
//            if (done.compareAndSet(false, true)) {
            child.onError(t);
//                long now = decr(start);
            if (t instanceof TransportException || t instanceof ClosedChannelException) {
                socket.dispose();
                concurrentSubscriptionTracker.removeLatencySubscriber(id);
            } else if (t instanceof TimeoutException) {
//                    updateQuantiles(now);
                concurrentSubscriptionTracker.removeLatencySubscriberAndUpdateQuantiles(id);
            }
//            }
        }

        @Override
        public void onComplete() {
//            if (done.compareAndSet(false, true)) {
//                long now = decr(start);
//                updateQuantiles(now);
            child.onComplete();
            concurrentSubscriptionTracker.removeLatencySubscriberAndUpdateQuantiles(id);
//            }
        }

//        private void updateQuantiles(long now) {
//            updateQuantiles.accept((double) now - start);
//        }
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
//            weightingStatistics.addStream();
            child.onSubscribe(s);
            concurrentSubscriptionTracker.addCountingSubscriber(id);
        }

        @Override
        public void onNext(U u) {
            child.onNext(u);
        }

        @Override
        public void onError(Throwable t) {
//            weightingStatistics.removeStream();
            child.onError(t);
            concurrentSubscriptionTracker.removeCountingSubscriber(id);
            if (t instanceof TransportException || t instanceof ClosedChannelException) {
                logger.debug("Disposing {} from activeSockets because of error {}", socket, t);
                socket.dispose();
            }
        }

        @Override
        public void onComplete() {
//            weightingStatistics.removeStream();
            child.onComplete();
            concurrentSubscriptionTracker.removeCountingSubscriber(id);
        }
    }

}
