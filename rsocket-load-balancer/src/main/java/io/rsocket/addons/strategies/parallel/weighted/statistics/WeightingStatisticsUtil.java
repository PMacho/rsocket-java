package io.rsocket.addons.strategies.parallel.weighted.statistics;

import io.rsocket.stat.Median;

import java.util.function.Supplier;

public class WeightingStatisticsUtil {

    private static final double STARTUP_PENALTY = Long.MAX_VALUE >> 12;

    public static double getPredictedLatency(
            final long pending,
            final long elapsed,
            final long inactivityFactor,
            final Median median,
            final double interArrivalTime,
            final Supplier<Long> instantaneous) {
        final long limitedElapsed = Math.max(elapsed, 1L);

        final double prediction = median.estimation();

        double weight;
        if (prediction == 0.0) {
            if (pending == 0) {
                weight = 0.0; // first request
            } else {
                // subsequent requests while we don't have any history
                weight = STARTUP_PENALTY + pending;
            }
        } else if (pending == 0 && limitedElapsed > inactivityFactor * interArrivalTime) {
            // if we did't see any data for a while, we decay the prediction by inserting
            // artificial 0.0 into the median
            median.insert(0.0);
            weight = median.estimation();
        } else {
            final double predicted = prediction * pending;
            final double instant = instantaneous.get();

            if (predicted < instant) { // NB: (0.0 < 0.0) == false
                weight = instant / pending; // NB: pending never equal 0 here
            } else {
                // we are under the predictions
                weight = prediction;
            }
        }

        return weight;
    }

    public static double algorithmicWeight(
            final double lowerQuantile,
            final double higherQuantile,
            final double predictedLatency,
            final long pending,
            final double exponentialFactor,
            final double availability) {
        // fixme: What is this good for? Should this really be need?
        // ensure higherQuantile > lowerQuantile + .1%
        final double high = Math.max(higherQuantile, lowerQuantile * 1.001);
        final double bandWidth = Math.max(high - lowerQuantile, 1);

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

        return availability * 1.0 / (1.0 + latency * (pending + 1));
    }

    public static class LatencyVariables {
        private final double predictedLatency;
        private final long pending;

        public LatencyVariables(double predictedLatency, long pending) {
            this.predictedLatency = predictedLatency;
            this.pending = pending;
        }

        public double getPredictedLatency() {
            return predictedLatency;
        }

        public long getPending() {
            return pending;
        }
    }
}
