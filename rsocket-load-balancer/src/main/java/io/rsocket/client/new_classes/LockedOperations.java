package io.rsocket.client.new_classes;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class LockedOperations {

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private final ExecutorService executorService;

    LockedOperations() {
        executorService = Executors.newCachedThreadPool();
    }

    protected void write(Runnable runnable) {
        executorService.execute(() -> {
            writeLock.lock();
            try {
                runnable.run();
            } finally {
                writeLock.unlock();
            }
        });
    }

    protected <S> Future<S> read(Callable<S> callable) {
        return executorService.submit(() -> {
            readLock.lock();
            try {
                return callable.call();
            } finally {
                writeLock.unlock();
            }
        });
    }

    protected <S> CompletableFuture<S> readCompletable(Supplier<S> supplier) {
        return CompletableFuture.supplyAsync(
                () -> {
                    readLock.lock();
                    try {
                        return supplier.get();
                    } finally {
                        writeLock.unlock();
                    }
                },
                executorService
        );
    }

}
