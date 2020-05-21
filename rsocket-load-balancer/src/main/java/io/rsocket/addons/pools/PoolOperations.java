package io.rsocket.addons.pools;

import io.rsocket.addons.RSocketPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;

public interface PoolOperations {

    Logger logger = LoggerFactory.getLogger(RSocketPool.class);

    DirectProcessor<Void> sourceControl = DirectProcessor.create();

    default <T> Flux<T> hotSource(Flux<T> flux) {
        return flux
                .share()
                .cache(1)
                .takeUntilOther(sourceControl);
    }

    default <T> Mono<T> snapshot(Flux<T> flux) {
        return flux.next();
    }

    default <T> Disposable poolUpdater(Flux<List<T>> flux, Consumer<List<T>> consumer) {
        return flux
                .onErrorContinue((throwable, o) -> logger.error("Received error signal for " + o, throwable))
                .subscribe(consumer);
    }
}
