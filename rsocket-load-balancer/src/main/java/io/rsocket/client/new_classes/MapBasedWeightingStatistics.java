package io.rsocket.client.new_classes;

import io.rsocket.stat.Ewma;
import io.rsocket.stat.Median;
import io.rsocket.util.Clock;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MapBasedWeightingStatistics {

    private static final int DEFAULT_INTER_ARRIVAL_FACTOR = 500;
    private static final long DEFAULT_INITIAL_INTER_ARRIVAL_TIME =
            Clock.unit().convert(1L, TimeUnit.SECONDS);
    private static final double STARTUP_PENALTY = Long.MAX_VALUE >> 12;

    private final long inactivityFactor;

    private ConcurrentPerformanceTracker concurrentPerformanceTracker = new ConcurrentPerformanceTracker();

    private final Median median;
    private final Ewma interArrivalTime;

    // todo: was called pending streams before, is anyways not used at present
    private final AtomicLong openStreams;

    MapBasedWeightingStatistics(
            long inactivityFactor
    ) {
        this.median = new Median();
        this.inactivityFactor = inactivityFactor;
        this.interArrivalTime = new Ewma(1, TimeUnit.MINUTES, DEFAULT_INITIAL_INTER_ARRIVAL_TIME);
        this.openStreams = new AtomicLong(0L);
    }

    MapBasedWeightingStatistics(
    ) {
        this(DEFAULT_INTER_ARRIVAL_FACTOR);
    }

    double getPredictedLatency() {
        final long now = now();
        final long elapsed = Math.max(now - concurrentPerformanceTracker.timeElapsed(), 1L);
        final long pending = concurrentPerformanceTracker.pending();

        final double prediction = median.estimation();

        double weight;
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
            final double predicted = prediction * pending;
            final double instant = instantaneous(now);

            if (predicted < instant) { // NB: (0.0 < 0.0) == false
                weight = instant / pending; // NB: pending never equal 0 here
            } else {
                // we are under the predictions
                weight = prediction;
            }
        }

        return weight;
    }

    private long instantaneous(long now) {
        return concurrentPerformanceTracker.getTrackedValues().stream().mapToLong(start -> Math.max(0, now - start)).sum();
    }

    void incr(UUID subscriberId, long now) {
        interArrivalTime.insert();
        concurrentPerformanceTracker.put(subscriberId, now);
    }

    void decr(UUID subscriberId, long now) {
        concurrentPerformanceTracker.remove(subscriberId);
    }

    void updateMedian(double rtt) {
        median.insert(rtt);
    }

    void addStream() {
        openStreams.incrementAndGet();
    }

    void removeStream() {
        openStreams.decrementAndGet();
    }

    private long now() {
        return Clock.now();
    }
}
