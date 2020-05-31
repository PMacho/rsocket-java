package io.rsocket.addons.strategies.parallel.weighted.statistics;

import io.rsocket.addons.strategies.util.LockedOperations;
import io.rsocket.stat.FrugalQuantile;

import java.util.concurrent.atomic.AtomicReference;

import reactor.core.publisher.Mono;

public class WeightedRSocketPoolStatistics extends LockedOperations {

    static final double DEFAULT_LOWER_QUANTILE = 0.2;
    static final double DEFAULT_HIGHER_QUANTILE = 0.8;

    private final AtomicReference<QuantilesWrapper> quantilesReference;

    public WeightedRSocketPoolStatistics() {
        super();
        this.quantilesReference = new AtomicReference<>(
                new QuantilesWrapper(DEFAULT_LOWER_QUANTILE, DEFAULT_HIGHER_QUANTILE)
        );
    }

    public void updateQuantiles(double rtt) {
        write(() -> quantilesReference.updateAndGet(q -> q.update(rtt)));
    }

    public Mono<Quantiles> getQuantiles() {
        return Mono.fromFuture(
                read(() -> {
                    final QuantilesWrapper quantilesWrapper = quantilesReference.get();
                    return new Quantiles(quantilesWrapper.getLowerQuantile(), quantilesWrapper.getHigherQuantile());
                }));
    }

    static class QuantilesWrapper {
        private FrugalQuantile lowerQuantile;
        private FrugalQuantile higherQuantile;

        QuantilesWrapper(double initialLowerQuantile, double initialHigherQuantile) {
            this.lowerQuantile = new FrugalQuantile(initialLowerQuantile);
            this.higherQuantile = new FrugalQuantile(initialHigherQuantile);
        }

        QuantilesWrapper update(double rtt) {
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

    public static class Quantiles {
        private final double lowerQuantile;
        private final double higherQuantile;

        public Quantiles(final double lowerQuantile, final double higherQuantile) {
            this.lowerQuantile = lowerQuantile;
            this.higherQuantile = higherQuantile;
        }

        public double getHigherQuantile() {
            return higherQuantile;
        }

        public double getLowerQuantile() {
            return lowerQuantile;
        }
    }
}