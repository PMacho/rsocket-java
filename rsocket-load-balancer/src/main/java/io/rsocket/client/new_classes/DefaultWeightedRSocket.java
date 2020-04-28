package io.rsocket.client.new_classes;


import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.client.TimeoutException;
import io.rsocket.client.TransportException;
import io.rsocket.client.filter.RSocketSupplier;
import io.rsocket.stat.Ewma;
import io.rsocket.stat.Median;
import io.rsocket.stat.Quantile;
import io.rsocket.util.Clock;
import io.rsocket.util.RSocketProxy;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class DefaultWeightedRSocket extends RSocketProxy implements WeightedRSocket {

    Logger logger = LoggerFactory.getLogger(DefaultWeightedRSocket.class);

    private static final double STARTUP_PENALTY = Long.MAX_VALUE >> 12;
    private static final long DEFAULT_INITIAL_INTER_ARRIVAL_TIME =
            Clock.unit().convert(1L, TimeUnit.SECONDS);
    private static final int DEFAULT_INTER_ARRIVAL_FACTOR = 500;
    private static final double DEFAULT_EXPONENTIAL_FACTOR = 4.0;

    //    private final Quantile lowerQuantile;
//    private final Quantile higherQuantile;
    private final Consumer<Double> updateQuantiles;
    private final Supplier<Statistics.Quantiles> quantilesSupplier;
    //    private final RSocket rSocket;
    private final long inactivityFactor;
    //    private final MonoProcessor<RSocket> rSocketMono;
    // fixme: make Atomic
    private volatile int pending; // instantaneous rate
    private long stamp; // last timestamp we sent a request
    private long stamp0; // last timestamp we sent a request or receive a response
    private long duration; // instantaneous cumulative duration

    private Median median;
    private Ewma interArrivalTime;

    private AtomicLong pendingStreams; // number of active streams

    private volatile double availability = 0.0;
    private final double exponentialFactor;

    DefaultWeightedRSocket(
            Consumer<Double> updateQuantiles,
            Supplier<Statistics.Quantiles> quantilesSupplier,
            RSocket rSocket,
            int inactivityFactor
    ) {
        super(rSocket);
        this.updateQuantiles = updateQuantiles;
        this.quantilesSupplier = quantilesSupplier;
//        this.rSocket = rSocket;
        this.inactivityFactor = inactivityFactor;

        long now = Clock.now();
        this.stamp = now;
        this.stamp0 = now;
        this.duration = 0L;
        this.pending = 0;
        this.median = new Median();
        this.interArrivalTime = new Ewma(1, TimeUnit.MINUTES, DEFAULT_INITIAL_INTER_ARRIVAL_TIME);
        this.pendingStreams = new AtomicLong();

        availability = 1.0;
        exponentialFactor = DEFAULT_EXPONENTIAL_FACTOR;

        logger.debug("Creating WeightedRSocket {} for RSocket {}", DefaultWeightedRSocket.this, rSocket);
    }

    DefaultWeightedRSocket(
            Consumer<Double> updateQuantiles,
            Supplier<Statistics.Quantiles> quantilesSupplier,
            RSocket rSocket
    ) {
        this(updateQuantiles, quantilesSupplier, rSocket, DEFAULT_INTER_ARRIVAL_FACTOR);
    }

    WeightedSocket(
            RSocketSupplier factory,
            Quantile lowerQuantile,
            Quantile higherQuantile,
            int inactivityFactor
    ) {
        this.rSocketMono = MonoProcessor.create();
        this.lowerQuantile = lowerQuantile;
        this.higherQuantile = higherQuantile;
        this.inactivityFactor = inactivityFactor;
        long now = Clock.now();
        this.stamp = now;
        this.stamp0 = now;
        this.duration = 0L;
        this.pending = 0;
        this.median = new Median();
        this.interArrivalTime = new Ewma(1, TimeUnit.MINUTES, DEFAULT_INITIAL_INTER_ARRIVAL_TIME);
        this.pendingStreams = new AtomicLong();

        logger.debug("Creating WeightedSocket {} from factory {}", WeightedSocket.this, factory);

        WeightedSocket.this
                .onClose()
                .doFinally(
                        s -> {
                            pool.accept(factory);
                            activeSockets.remove(WeightedSocket.this);
                            logger.debug(
                                    "Removed {} from factory {} from activeSockets", WeightedSocket.this, factory);
                        })
                .subscribe();

        factory
                .get()
                .retryBackoff(weightedSocketRetries, weightedSocketBackOff, weightedSocketMaxBackOff)
                .doOnError(
                        throwable -> {
                            logger.error(
                                    "error while connecting {} from factory {}",
                                    WeightedSocket.this,
                                    factory,
                                    throwable);
                            WeightedSocket.this.dispose();
                        })
                .subscribe(
                        rSocket -> {
                            // When RSocket is closed, close the WeightedSocket
                            rSocket
                                    .onClose()
                                    .doFinally(
                                            signalType -> {
                                                logger.info(
                                                        "RSocket {} from factory {} closed", WeightedSocket.this, factory);
                                                WeightedSocket.this.dispose();
                                            })
                                    .subscribe();

                            // When the factory is closed, close the RSocket
                            factory
                                    .onClose()
                                    .doFinally(
                                            signalType -> {
                                                logger.info("Factory {} closed", factory);
                                                rSocket.dispose();
                                            })
                                    .subscribe();

                            // When the WeightedSocket is closed, close the RSocket
                            WeightedSocket.this
                                    .onClose()
                                    .doFinally(
                                            signalType -> {
                                                logger.info(
                                                        "WeightedSocket {} from factory {} closed",
                                                        WeightedSocket.this,
                                                        factory);
                                                rSocket.dispose();
                                            })
                                    .subscribe();

                /*synchronized (LoadBalancedRSocketMono.this) {
                  if (activeSockets.size() >= targetAperture) {
                    quickSlowestRS();
                    pendingSockets -= 1;
                  }
                }*/
                            rSocketMono.onNext(rSocket);
                            availability = 1.0;
                            if (!WeightedSocket.this
                                    .isDisposed()) { // May be already disposed because of retryBackoff delay
                                activeSockets.add(WeightedSocket.this);
                                logger.debug(
                                        "Added WeightedSocket {} from factory {} to activeSockets",
                                        WeightedSocket.this,
                                        factory);
                            }
                        });
    }

    @Override
    public double algorithmicWeight() {
        if (availability() == 0.0) {
            return 0.0;
        }

        int pendings = getPending();
        double latency = getPredictedLatency();

        final Statistics.Quantiles quantiles = quantilesSupplier.get();

        double low = quantiles.getLowerQuantile();
        // ensure higherQuantile > lowerQuantile + .1%
        double high = Math.max(quantiles.getHigherQuantile(), low * 1.001);

        double bandWidth = Math.max(high - low, 1);

        if (latency < low) {
            double alpha = (low - latency) / bandWidth;
            double bonusFactor = Math.pow(1 + alpha, exponentialFactor);
            latency /= bonusFactor;
        } else if (latency > high) {
            double alpha = (latency - high) / bandWidth;
            double penaltyFactor = Math.pow(1 + alpha, exponentialFactor);
            latency *= penaltyFactor;
        }

        return source.availability() * 1.0 / (1.0 + latency * (pendings + 1));

    }

    private <U> LatencySubscriber<U> latencySubscriber(Subscriber<U> child, WeightedRSocket socket) {
        return new LatencySubscriber<>(
                child,
                socket,
                rtt -> {
                    // fixme: make atomic
                    median.insert(rtt);
                    updateQuantiles.accept(rtt);
                }
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

        return Flux.from(subscriber -> source
                .requestStream(payload)
                .subscribe(new CountingSubscriber<>(subscriber, this))
        );
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        return Mono.from(subscriber -> source
                .fireAndForget(payload)
                .subscribe(new CountingSubscriber<>(subscriber, this))
        );
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
        return Mono.from(subscriber -> source
                .metadataPush(payload)
                .subscribe(new CountingSubscriber<>(subscriber, this))
        );
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return Flux.from(subscriber -> source
                .requestChannel(payloads)
                .subscribe(new CountingSubscriber<>(subscriber, this))
        );
    }

    synchronized double getPredictedLatency() {
        long now = Clock.now();
        long elapsed = Math.max(now - stamp, 1L);

        double weight;
        double prediction = median.estimation();

        if (prediction == 0.0) {
            if (pending == 0) {
                weight = 0.0; // first request
            } else {
                // subsequent requests while we don't have any history
                weight = STARTUP_PENALTY + pending;
            }
        } else if (pending == 0 && elapsed > inactivityFactor * interArrivalTime.value()) {
            // if we did't see any data for a while, we decay the prediction by inserting
            // artificial 0.0 into the median
            median.insert(0.0);
            weight = median.estimation();
        } else {
            double predicted = prediction * pending;
            double instant = instantaneous(now);

            if (predicted < instant) { // NB: (0.0 < 0.0) == false
                weight = instant / pending; // NB: pending never equal 0 here
            } else {
                // we are under the predictions
                weight = prediction;
            }
        }

        return weight;
    }

    int getPending() {
        return pending;
    }

    // fixme: statistics?
    private synchronized long instantaneous(long now) {
        return duration + (now - stamp0) * pending;
    }

    // fixme: statistics?
    private synchronized long incr() {
        long now = Clock.now();
        interArrivalTime.insert(now - stamp);
        duration += Math.max(0, now - stamp0) * pending;
        pending += 1;
        stamp = now;
        stamp0 = now;
        return now;
    }

    // fixme: statistics?
    private synchronized long decr(long timestamp) {
        long now = Clock.now();
        duration += Math.max(0, now - stamp0) * pending - (now - timestamp);
        pending -= 1;
        stamp0 = now;
        return now;
    }

    private synchronized void observe(double rtt) {
        median.insert(rtt);
        updateQuantiles.accept(rtt);
//        lowerQuantile.insert(rtt);
//        higherQuantile.insert(rtt);
    }

    @Override
    public double availability() {
        return availability;
    }

    @Override
    public String toString() {
        return "WeightedSocket("
                + "median="
                + median.estimation()
                + " quantile-low="
                + quantilesSupplier.get().getLowerQuantile()
                + " quantile-high="
                + quantilesSupplier.get().getHigherQuantile()
                + " inter-arrival="
                + interArrivalTime.value()
                + " duration/pending="
                + (pending == 0 ? 0 : (double) duration / pending)
                + " pending="
                + pending
                + " availability= "
                + availability()
                + ")->";
    }

//    @Override
//    public double medianLatency() {
//        return median.estimation();
//    }
//
//    @Override
//    public double lowerQuantileLatency() {
//        return lowerQuantile.estimation();
//    }
//
//    @Override
//    public double higherQuantileLatency() {
//        return higherQuantile.estimation();
//    }
//
//    @Override
//    public double interArrivalTime() {
//        return interArrivalTime.value();
//    }
//
//    @Override
//    public int pending() {
//        return pending;
//    }
//
//    @Override
//    public long lastTimeUsedMillis() {
//        return stamp0;
//    }

    /**
     * Subscriber wrapper used for request/response interaction model, measure and collect latency
     * information.
     */
    private class LatencySubscriber<U> implements Subscriber<U> {
        private final Subscriber<U> child;
        private final WeightedRSocket socket;
        private final AtomicBoolean done;
        private long start;
        private final Consumer<Double> updateQuantiles;

        LatencySubscriber(Subscriber<U> child, WeightedRSocket socket, Consumer<Double> updateQuantiles;) {
            this.child = child;
            this.socket = socket;
            this.done = new AtomicBoolean(false);
            this.updateQuantiles = updateQuantiles;
        }

        @Override
        public void onSubscribe(Subscription s) {
            start = incr();
            child.onSubscribe(
                    new Subscription() {
                        @Override
                        public void request(long n) {
                            s.request(n);
                        }

                        @Override
                        public void cancel() {
                            if (done.compareAndSet(false, true)) {
                                s.cancel();
                                decr(start);
                            }
                        }
                    });
        }

        @Override
        public void onNext(U u) {
            child.onNext(u);
        }

        @Override
        public void onError(Throwable t) {
            if (done.compareAndSet(false, true)) {
                child.onError(t);
                long now = decr(start);
                if (t instanceof TransportException || t instanceof ClosedChannelException) {
                    socket.dispose();
                } else if (t instanceof TimeoutException) {
                    updateQuantiles(now);
                }
            }
        }

        @Override
        public void onComplete() {
            if (done.compareAndSet(false, true)) {
                long now = decr(start);
                updateQuantiles(now);
                child.onComplete();
            }
        }

        private void updateQuantiles(long now) {
            updateQuantiles.accept((double) now - start);
        }
    }

    /**
     * Subscriber wrapper used for stream like interaction model, it only counts the number of
     * active streams
     */
    private class CountingSubscriber<U> implements Subscriber<U> {
        private final Subscriber<U> child;
        private final WeightedRSocket socket;

        CountingSubscriber(Subscriber<U> child, WeightedRSocket socket) {
            this.child = child;
            this.socket = socket;
        }

        @Override
        public void onSubscribe(Subscription s) {
            socket.pendingStreams.incrementAndGet();
            child.onSubscribe(s);
        }

        @Override
        public void onNext(U u) {
            child.onNext(u);
        }

        @Override
        public void onError(Throwable t) {
            socket.pendingStreams.decrementAndGet();
            child.onError(t);
            if (t instanceof TransportException || t instanceof ClosedChannelException) {
                logger.debug("Disposing {} from activeSockets because of error {}", socket, t);
                socket.dispose();
            }
        }

        @Override
        public void onComplete() {
            socket.pendingStreams.decrementAndGet();
            child.onComplete();
        }
    }

}
