/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.addons.strategies.weighted.statistics;

import io.rsocket.addons.strategies.weighted.basic.AtomicDouble;
import io.rsocket.util.Clock;

import java.util.concurrent.TimeUnit;

/**
 * Compute the exponential weighted moving average of a series of values. The time at which you
 * insert the value into `Ewma` is used to compute a weight (recent points are weighted higher). The
 * parameter for defining the convergence speed (like most decay process) is the half-life.
 *
 * <p>e.g. with a half-life of 10 unit, if you insert 100 at t=0 and 200 at t=10 the ewma will be
 * equal to (200 - 100)/2 = 150 (half of the distance between the new and the old value)
 */
public class AtomicEwma {
    private final long tau;
    private AtomicDouble value;

    public AtomicEwma(long halfLife, TimeUnit unit, double initialValue) {
        this.tau = Clock.unit().convert((long) (halfLife / Math.log(2)), unit);
        value = new AtomicDouble(initialValue);
    }

    public void insert(long elapsed) {
        insert(elapsed, elapsed);
    }

    public void insert(long observation, long elapsed) {
        value.updateAndGet(current -> {
            double w = 1. / Math.exp(((double) elapsed) / tau);
            return w * current + (1.0 - w) * observation;
        });
    }

    //    public synchronized void insert() {
    //        long now = Clock.now();
    //        insert(Math.max(0, now - stamp));
    //    }
    //
    //    public synchronized void insert(double x) {
    //        long now = Clock.now();
    //        double elapsed = Math.max(0, now - stamp);
    //        stamp = now;
    //
    //        double w = Math.exp(-elapsed / tau);
    //        ewma = w * ewma + (1.0 - w) * x;
    //    }

    public synchronized void reset(double value) {
        this.value.set(value);
    }

    public double value() {
        return value.get();
    }

    @Override
    public String toString() {
        return "Ewma(value=" + value + ")";
    }
}
