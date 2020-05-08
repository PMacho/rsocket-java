package io.rsocket.client.new_classes;

import io.rsocket.stat.Ewma;
import io.rsocket.stat.Median;
import io.rsocket.util.Clock;

import java.util.UUID;
import java.util.function.Supplier;

public class WeightingStatisticsUtil {

    private static final double STARTUP_PENALTY = Long.MAX_VALUE >> 12;

    static double getPredictedLatency(
            final long pending,
            final long elapsed,
            final long inactivityFactor,
            final Median median,
            final double interArrivalTime,
            final Supplier<Long> instantaneous
    ) {
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

}
