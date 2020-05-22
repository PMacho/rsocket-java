package io.rsocket.addons.strategies.weighted.basic;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public abstract class LockedOperations {

  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);
  private final Lock readLock = rwLock.readLock();
  private final Lock writeLock = rwLock.writeLock();

  private final ExecutorService executorService;

  public LockedOperations() {
    executorService = Executors.newCachedThreadPool();
  }

  protected void write(Runnable runnable) {
    run(runnable, writeLock);
  }

  protected void read(Runnable runnable) {
    run(runnable, readLock);
  }

  protected <S> CompletableFuture<S> write(Supplier<S> supplier) {
    return supply(supplier, writeLock);
  }

  protected <S> CompletableFuture<S> read(Supplier<S> supplier) {
    return supply(supplier, readLock);
  }

  public <S> Supplier<CompletableFuture<S>> consume(Supplier<S> mapper) {
    return () -> read(mapper);
  }

  protected <S> CompletableFuture<S> supply(Supplier<S> supplier, Lock lock) {
    return CompletableFuture.supplyAsync(
        () -> {
          lock.lock();
          try {
            return supplier.get();
          } finally {
            writeLock.unlock();
          }
        },
        executorService);
  }

  protected void run(Runnable runnable, Lock lock) {
    CompletableFuture.runAsync(
        () -> {
          lock.lock();
          try {
            runnable.run();
          } finally {
            writeLock.unlock();
          }
        },
        executorService);
  }
}
