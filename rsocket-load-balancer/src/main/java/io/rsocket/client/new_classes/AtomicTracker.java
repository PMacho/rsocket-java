package io.rsocket.client.new_classes;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class AtomicTracker {

    private static final long tombstone = -1;

    private static final Predicate<Long> isTombstone = start -> start == tombstone;

    private final ConcurrentHashMap<UUID, Long> trackerMap = new ConcurrentHashMap<>();

    public AtomicTracker() {
        Executors
                .newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(this::compaction, 0, 100, TimeUnit.MILLISECONDS);
    }

    public void put(UUID uuid, Long timestamp) {
        trackerMap.putIfAbsent(uuid, timestamp);
    }

    public void remove(UUID uuid) {
        trackerMap.replace(uuid, tombstone);
    }

    public Stream<Long> startTimes() {
        return trackerMap.values().stream().filter(isTombstone.negate());
    }

    public long trackedObjectsCount() {
        return startTimes().count();
    }

    private void compaction() {
        trackerMap.values().removeIf(isTombstone);
    }
}
