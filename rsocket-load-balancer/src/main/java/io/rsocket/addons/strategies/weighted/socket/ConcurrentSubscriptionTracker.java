package io.rsocket.addons.strategies.weighted.socket;

import io.rsocket.client.basic.LockedOperations;
import io.rsocket.stat.AtomicEwma;
import io.rsocket.stat.Median;
import io.rsocket.util.Clock;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import reactor.core.publisher.Mono;

public class ConcurrentSubscriptionTracker extends LockedOperations {

  private static final int DEFAULT_INTER_ARRIVAL_FACTOR = 500;
  private static final long DEFAULT_INITIAL_INTER_ARRIVAL_TIME =
      Clock.unit().convert(1L, TimeUnit.SECONDS);

  private final long inactivityFactor;
  // todo: make me atomice
  private final AtomicReference<Median> median = new AtomicReference<>(new Median());
  private final AtomicEwma interArrivalTime;

  private final AtomicTracker requestTracker = new AtomicTracker();
  private final AtomicLong latestRequestTime = new AtomicLong(now());
  // todo: this is not used at present
  private final AtomicTracker streamTracker = new AtomicTracker();

  private final Consumer<Double> updateQuantiles;

  ConcurrentSubscriptionTracker(
      final Consumer<Double> updateQuantiles, final long inactivityFactor) {
    super();
    this.updateQuantiles = updateQuantiles;
    this.inactivityFactor = inactivityFactor;
    this.interArrivalTime = new AtomicEwma(1, TimeUnit.MINUTES, DEFAULT_INITIAL_INTER_ARRIVAL_TIME);
  }

  ConcurrentSubscriptionTracker(final Consumer<Double> updateQuantiles) {
    this(updateQuantiles, DEFAULT_INTER_ARRIVAL_FACTOR);
  }

  public <T> Mono<T> consume(Function<ConcurrentSubscriptionTracker, T> mapper) {
    return Mono.fromFuture(read(() -> mapper.apply(this)));
  }

  public double predictedLatency() {
    return WeightingStatisticsUtil.getPredictedLatency(
        pending(),
        elapsed(),
        inactivityFactor,
        median.get(),
        interArrivalTime.value(),
        this::instantaneous);
  }

  public void addLatencySubscriber(UUID uuid) { // , final long timestamp) {
    final long timestamp = now();
    write(
        () -> {
          interArrivalTime.insert(elapsed(timestamp));
          requestTracker.put(uuid, timestamp);
          latestRequestTime.updateAndGet(current -> Math.max(current, timestamp));
        });
  }

  public Mono<Long> removeLatencySubscriber(UUID uuid) {
    return requestTracker.remove(uuid);
  }

  public void removeLatencySubscriberAndUpdateQuantiles(UUID uuid) {
    final long now = now();
    removeLatencySubscriber(uuid)
        .subscribe(
            start -> {
              final double rtt = (double) now - start;
              updateQuantiles.accept(rtt);
              updateMedian(rtt);
            });
  }

  public void addCountingSubscriber(UUID uuid) {
    write(() -> streamTracker.put(uuid, now()));
  }

  public void removeCountingSubscriber(UUID uuid) {
    write(() -> streamTracker.remove(uuid));
  }

  public void updateMedian(double rtt) {
    write(() -> median.get().insert(rtt));
  }

  private long instantaneous() {
    return requestTracker.startTimes().mapToLong(start -> Math.max(0, now() - start)).sum();
  }

  public long pending() {
    return requestTracker.trackedObjectsCount();
  }

  private long elapsed() {
    return elapsed(now());
  }

  private long elapsed(final long timestamp) {
    return Math.max(0, timestamp - latestRequestTime.get());
  }

  private long now() {
    return Clock.now();
  }
}
