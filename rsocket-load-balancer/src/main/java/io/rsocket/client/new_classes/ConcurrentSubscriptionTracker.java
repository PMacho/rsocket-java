package io.rsocket.client.new_classes;

import io.rsocket.stat.Ewma;
import io.rsocket.stat.Median;
import io.rsocket.util.Clock;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ConcurrentSubscriptionTracker {

    private static final int DEFAULT_INTER_ARRIVAL_FACTOR = 500;
    private static final long DEFAULT_INITIAL_INTER_ARRIVAL_TIME =
            Clock.unit().convert(1L, TimeUnit.SECONDS);

    private final long inactivityFactor;
    private final Median median;
    private final Ewma interArrivalTime;

    private final ConcurrentOperations<ConcurrentSubscriptionTracker> concurrentOperations;

    private final ConcurrentHashMap<UUID, Long> subscriptionMap = new ConcurrentHashMap<>();
    private final ConcurrentSkipListSet<UUID> tombstones = new ConcurrentSkipListSet<>();

    private long latestStart = now();

    ConcurrentSubscriptionTracker() {
        this.concurrentOperations = new ConcurrentOperations<>(this);
        this.median = new Median();
        this.inactivityFactor = inactivityFactor;
        this.interArrivalTime = new Ewma(1, TimeUnit.MINUTES, DEFAULT_INITIAL_INTER_ARRIVAL_TIME);
        this.openStreams = new AtomicLong(0L);
    }

    ConcurrentSubscriptionTracker(
    ) {
        this(DEFAULT_INTER_ARRIVAL_FACTOR);
    }

    public void incr(UUID uuid, Long value) {
        concurrentOperations.write(tracker -> tracker.put(uuid, value));
    }

    public void decr(UUID uuid) {
        concurrentOperations.write(tracker -> tracker.remove(uuid));
    }

    public double predictedLatency() {
        return concurrentOperations.read(
                tracker -> WeightingStatisticsUtil.getPredictedLatency(
                        tracker.pending(),
                        tracker.elapsed(),
                        tracker.inactivityFactor,
                        tracker.median,
                        tracker.interArrivalTime,
                        tracker::instantaneous
                )
        );
    }

    private void put(UUID uuid, Long value) {
        if (!tombstones.remove(uuid)) {
            subscriptionMap.put(uuid, value);
            updateStart();
        }
    }

    private void remove(UUID uuid) {
        final Long removed = subscriptionMap.remove(uuid);
        if (Objects.isNull(removed))
            tombstones.add(uuid);
        else
            updateStart();
    }

    private long instantaneous() {
        return subscriptionMap.values().stream().mapToLong(start -> Math.max(0, now() - start)).sum();
    }

    private int pending() {
        return subscriptionMap.size();
    }

    private long elapsed() {
        return now() - latestStart;
    }

    private void updateStart() {
        latestStart = Collections.min(subscriptionMap.values(), Comparator.comparingLong(Long::valueOf));
    }

    private long now() {
        return Clock.now();
    }
}
