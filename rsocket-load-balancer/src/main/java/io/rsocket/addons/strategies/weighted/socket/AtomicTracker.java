package io.rsocket.addons.strategies.weighted.socket;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class AtomicTracker {

    private final ConcurrentHashMap<UUID, Long> trackerMap = new ConcurrentHashMap<>();

    public AtomicTracker() {
    }

    public void put(UUID uuid, Long timestamp) {
        trackerMap.putIfAbsent(uuid, timestamp);
    }

    public Mono<Long> remove(UUID uuid) {
        return Flux
                .interval(Duration.ZERO, Duration.ofMillis(5))
                .filter(i -> trackerMap.containsKey(uuid))
                .take(1)
                .then(Mono.fromCallable(() -> trackerMap.remove(uuid)));
    }

    public Stream<Long> startTimes() {
        return trackerMap.values().stream();
    }

    public long trackedObjectsCount() {
        return trackerMap.size();
    }

}
