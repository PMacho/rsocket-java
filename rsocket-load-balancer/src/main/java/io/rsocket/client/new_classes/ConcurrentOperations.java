package io.rsocket.client.new_classes;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

public class ConcurrentOperations<T> {

    private final AtomicReference<T> statistics;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    ConcurrentOperations(T t) {
        statistics = new AtomicReference<>(t);
    }

    public T write(Consumer<T> mapper) {
        return statistics.updateAndGet(ws -> {
            writeLock.lock();
            try {
                mapper.accept(ws);
                return ws;
            } finally {
                writeLock.unlock();
            }
        });
    }

    public <S> S read(Function<T, S> mapper) {
        readLock.lock();
        try {
            return mapper.apply(statistics.get());
        } finally {
            writeLock.unlock();
        }
    }

}
