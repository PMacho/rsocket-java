package io.rsocket.client.new_classes;

import io.rsocket.stat.FrugalQuantile;

public class WeightedRSocketPoolStatistics {

    static final double DEFAULT_LOWER_QUANTILE = 0.2;
    static final double DEFAULT_HIGHER_QUANTILE = 0.8;

    private final Quantiles quantiles;

    WeightedRSocketPoolStatistics() {
        this.quantiles = new Quantiles(DEFAULT_LOWER_QUANTILE, DEFAULT_HIGHER_QUANTILE);
    }

    public void updateQuantiles(double rtt) {
        quantiles.update(rtt);
    }

    public Quantiles getQuantiles() {
        return quantiles;
    }

    static class Quantiles {
        private FrugalQuantile lowerQuantile;
        private FrugalQuantile higherQuantile;

        Quantiles(double initialLowerQuantile, double initialHigherQuantile) {
            this.lowerQuantile = new FrugalQuantile(initialLowerQuantile);
            this.higherQuantile = new FrugalQuantile(initialHigherQuantile);
        }

        WeightedRSocketPoolStatistics.Quantiles update(double rtt) {
            lowerQuantile.insert(rtt);
            higherQuantile.insert(rtt);
            return this;
        }

        double getLowerQuantile() {
            return lowerQuantile.estimation();
        }

        double getHigherQuantile() {
            return higherQuantile.estimation();
        }
    }

}
