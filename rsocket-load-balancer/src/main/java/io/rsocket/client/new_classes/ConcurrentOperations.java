package io.rsocket.client.new_classes;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

public class ConcurrentOperations<T> {

    private final AtomicReference<T> reference;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private final ExecutorService executorService;

    ConcurrentOperations(T t) {
        reference = new AtomicReference<>(t);
        executorService = Executors.newCachedThreadPool();
    }

    public void write(Consumer<T> mapper) {
        executorService.execute(() ->
                reference.updateAndGet(ws -> {
                    writeLock.lock();
                    try {
                        mapper.accept(ws);
                        return ws;
                    } finally {
                        writeLock.unlock();
                    }
                })
        );
    }

    public <S> S read(Function<T, S> mapper) {
        readLock.lock();
        try {
            return mapper.apply(reference.get());
        } finally {
            writeLock.unlock();
        }
    }

}
