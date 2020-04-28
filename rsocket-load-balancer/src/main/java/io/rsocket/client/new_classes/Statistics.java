package io.rsocket.client.new_classes;

import io.rsocket.stat.FrugalQuantile;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class Statistics {

    private final AtomicReference<Statistics.Quantiles> quantilesAtomicReference;
    static final double DEFAULT_LOWER_QUANTILE = 0.2;
    static final double DEFAULT_HIGHER_QUANTILE = 0.8;

    final Consumer<Double> updateQuantiles;
    final Supplier<Statistics.Quantiles> quantilesSupplier;

    Statistics() {
        quantilesAtomicReference = new AtomicReference<>(
                new Quantiles(DEFAULT_LOWER_QUANTILE, DEFAULT_HIGHER_QUANTILE)
        );

        updateQuantiles = this::updateQuantiles;
        quantilesSupplier = this::getQuantiles;
    }

    private void updateQuantiles(double rtt) {
        quantilesAtomicReference.getAndUpdate(quantiles -> quantiles.update(rtt));
    }

    private Quantiles getQuantiles() {
        return quantilesAtomicReference.get();
    }

    static class Quantiles {
        private FrugalQuantile lowerQuantile;
        private FrugalQuantile higherQuantile;

        Quantiles(double initialLowerQuantile, double initialHigherQuantile) {
            this.lowerQuantile = new FrugalQuantile(initialLowerQuantile);
            this.higherQuantile = new FrugalQuantile(initialHigherQuantile);
        }

        Statistics.Quantiles update(double rtt) {
            lowerQuantile.insert(rtt);
            higherQuantile.insert(rtt);
            return this;
        }

        double getLowerQuantile() {
            return lowerQuantile.estimation();
        }

        double getHigherQuantile(){
            return higherQuantile.estimation();
        }
    }

}
