package io.rsocket.client.new_classes;

import io.rsocket.stat.AtomicEwma;
import io.rsocket.stat.Median;
import io.rsocket.util.Clock;
import reactor.core.publisher.Mono;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ConcurrentPerformanceTracker extends LockedOperations {

    private static final int DEFAULT_INTER_ARRIVAL_FACTOR = 500;
    private static final long DEFAULT_INITIAL_INTER_ARRIVAL_TIME =
            Clock.unit().convert(1L, TimeUnit.SECONDS);

    private final long inactivityFactor;
    private final Median median;
    private final AtomicEwma interArrivalTime;

    private final AtomicTracker requestTracker = new AtomicTracker();
    private final AtomicLong latestRequestTime = new AtomicLong(now());
    // todo: this is not used at present
    private final AtomicTracker streamTracker = new AtomicTracker();


    ConcurrentPerformanceTracker(final long inactivityFactor) {
        super();
        this.median = new Median();
        this.inactivityFactor = inactivityFactor;
        this.interArrivalTime = new AtomicEwma(1, TimeUnit.MINUTES, DEFAULT_INITIAL_INTER_ARRIVAL_TIME);
    }

    ConcurrentPerformanceTracker(
    ) {
        this(DEFAULT_INTER_ARRIVAL_FACTOR);
    }

    public Mono<Double> predictedLatency() {
        return Mono.fromFuture(
                readCompletable(
                        () -> WeightingStatisticsUtil.getPredictedLatency(
                                pending(),
                                elapsed(),
                                inactivityFactor,
                                median,
                                interArrivalTime.value(),
                                this::instantaneous
                        )
                )
        );
    }

    public void addSubscription(UUID uuid, long timestamp) {
        write(() -> {
            interArrivalTime.insert(elapsed(timestamp));
            requestTracker.put(uuid, timestamp);
            latestRequestTime.updateAndGet(current -> Math.max(current, timestamp));
        });
    }

    public void removeSubscription(UUID uuid) {
        write(() -> requestTracker.remove(uuid));
    }

    public void addStream(UUID uuid) {
        write(() -> streamTracker.put(uuid, now()));
    }

    public void removeStream(UUID uuid) {
        write(() -> streamTracker.remove(uuid));
    }

    private long instantaneous() {
        return requestTracker.startTimes().mapToLong(start -> Math.max(0, now() - start)).sum();
    }

    private long pending() {
        return requestTracker.trackedObjectsCount();
    }

    private long elapsed() {
        return elapsed(now());
    }

    private long elapsed(long timestamp) {
        return Math.max(0, timestamp - latestRequestTime.get());
    }

    private long now() {
        return Clock.now();
    }
}
