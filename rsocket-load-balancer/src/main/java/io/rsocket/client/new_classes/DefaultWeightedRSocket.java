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
import java.util.function.Function;
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
    private final Function<Function<WeightedRSocketPoolStatistics.Quantiles, S>> doWithQuantiles;
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

    private final ConcurrentOperations<WeightingStatistics> weightingStatisticsOperations;
    private final ConcurrentOperations<WeightedRSocketPoolStatistics> weightedRSocketPoolStatisticsOperations;

    DefaultWeightedRSocket(
            Consumer<Double> updateQuantiles,
            ConcurrentOperations<WeightedRSocketPoolStatistics> weightedRSocketPoolStatisticsOperations,
            RSocket rSocket,
            int inactivityFactor
    ) {
        super(rSocket);
        this.updateQuantiles = rtt -> weightedRSocketPoolStatisticsOperations.write(
                weightedRSocketPoolStatistics -> weightedRSocketPoolStatistics.updateQuantiles(rtt)
        );
        this.weightedRSocketPoolStatisticsOperations = weightedRSocketPoolStatisticsOperations;
//        this.doWithQuantiles = doWithQuantiles;
//        this.rSocket = rSocket;
        this.inactivityFactor = inactivityFactor;

        availability = 1.0;
        exponentialFactor = DEFAULT_EXPONENTIAL_FACTOR;
        this.weightingStatisticsOperations = new ConcurrentOperations<>(new WeightingStatistics());

        logger.debug("Creating WeightedRSocket {} for RSocket {}", DefaultWeightedRSocket.this, rSocket);
    }

    DefaultWeightedRSocket(
            Consumer<Double> updateQuantiles,
            ConcurrentOperations<WeightedRSocketPoolStatistics> weightedRSocketPoolStatisticsOperations,
//            Consumer<Consumer<WeightedRSocketPoolStatistics.Quantiles>> doWithQuantiles,
            RSocket rSocket
    ) {
        this(updateQuantiles, weightedRSocketPoolStatisticsOperations, rSocket, DEFAULT_INTER_ARRIVAL_FACTOR);
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

        return weightedRSocketPoolStatisticsOperations.read(
                weightedRSocketPoolStatistics -> weightingStatisticsOperations.read(
                        weightingStatistics -> weight(
                                weightedRSocketPoolStatistics.getQuantiles(),
                                weightingStatistics)
                )
        );
    }

    private double weight(WeightedRSocketPoolStatistics.Quantiles quantiles, WeightingStatistics weightingStatistics) {
        final double low = quantiles.getLowerQuantile();
        // ensure higherQuantile > lowerQuantile + .1%
        final double high = Math.max(quantiles.getHigherQuantile(), low * 1.001);
        final double bandWidth = Math.max(high - low, 1);

        final int pending = weightingStatistics.getPending();
        double latency = weightingStatistics.getPredictedLatency();

        if (latency < low) {
            double alpha = (low - latency) / bandWidth;
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
                socket,
                rtt -> {
                    // fixme: make atomic
                    weightingStatisticsOperations.write(ws -> ws.updateMedian(rtt));
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

    private long incr() {
        long now = now();
        weightingStatisticsOperations.write(weightingStatistics -> weightingStatistics.incr(now));
        return now;
    }

    private long decr(long start) {
        long now = now();
        weightingStatisticsOperations.write(weightingStatistics -> weightingStatistics.decr(start, now));
        return now;
    }

    private long now() {
        return Clock.now();
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
                + doWithQuantiles.get().getLowerQuantile()
                + " quantile-high="
                + doWithQuantiles.get().getHigherQuantile()
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
