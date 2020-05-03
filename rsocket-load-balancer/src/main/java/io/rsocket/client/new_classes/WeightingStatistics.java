package io.rsocket.client.new_classes;

import io.rsocket.stat.Ewma;
import io.rsocket.stat.Median;
import io.rsocket.util.Clock;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class WeightingStatistics {

    private static final int DEFAULT_INTER_ARRIVAL_FACTOR = 500;
    private static final long DEFAULT_INITIAL_INTER_ARRIVAL_TIME =
            Clock.unit().convert(1L, TimeUnit.SECONDS);
    private static final double STARTUP_PENALTY = Long.MAX_VALUE >> 12;

    private final long inactivityFactor;
    private int pending; // instantaneous rate
    private long stamp; // last timestamp we sent a request
    private long stamp0; // last timestamp we sent a request or receive a response
    private long duration; // instantaneous cumulative duration

    private final Median median;
    private final Ewma interArrivalTime;

    WeightingStatistics(
    ) {
        final long now = Clock.now();
        this.stamp = now;
        this.stamp0 = now;
        this.duration = 0L;
        this.pending = 0;
        this.median = new Median();
        this.inactivityFactor = DEFAULT_INTER_ARRIVAL_FACTOR;
        this.interArrivalTime = new Ewma(1, TimeUnit.MINUTES, DEFAULT_INITIAL_INTER_ARRIVAL_TIME);
    }

    double getPredictedLatency() {
        final long now = now();
        final long elapsed = Math.max(now - stamp, 1L);

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

    int getPending() {
        return pending;
    }

    private long instantaneous(long now) {
        return duration + (now - stamp0) * pending;
    }

    void incr(long now) {
        interArrivalTime.insert(now - stamp);
        duration += Math.max(0, now - stamp0) * pending;
        pending += 1;
        stamp = now;
        stamp0 = now;
    }

    void decr(long start, long now) {
//        final long now = now();
        duration += Math.max(0, now - stamp0) * pending - (now - start);
        pending -= 1;
        stamp0 = now;
//        return now;
    }

    void updateMedian(double rtt) {
        median.insert(rtt);
    }

    private long now() {
        return Clock.now();
    }
}
