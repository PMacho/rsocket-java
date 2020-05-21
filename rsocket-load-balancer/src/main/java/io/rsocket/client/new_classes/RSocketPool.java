//package io.rsocket.client.new_classes;
//
//import io.rsocket.RSocket;
//import org.reactivestreams.Publisher;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import reactor.core.Disposable;
//import reactor.core.publisher.DirectProcessor;
//import reactor.core.publisher.Flux;
//import reactor.core.publisher.Mono;
//
//import java.util.Collection;
//import java.util.List;
//import java.util.function.Consumer;
//
//public interface RSocketPool<S extends RSocket> {
//
//    Logger logger = LoggerFactory.getLogger(RSocketPool.class);
//
//    DirectProcessor<Void> rSocketSourceControl = DirectProcessor.create();
//
//    void start(Publisher<? extends Collection<RSocket>> rSocketsPublisher);
//
//    Mono<S> select();
//
//    Flux<S> selectAll();
//
//    void update();
//
//    Mono<Void> clean();
//
//
//    default <T> Flux<T> hotSource(Flux<T> flux) {
//        return flux
//                .share()
//                .cache(1)
//                .takeUntilOther(rSocketSourceControl);
//    }
//
//    default <T> Mono<T> snapshot(Flux<T> flux) {
//        return Flux.from(flux).next();
//    }
//
//    default <T> Disposable poolUpdater(Flux<List<T>> flux, Consumer<List<T>> consumer) {
//        return flux
//                .onErrorContinue((throwable, o) -> logger.error("Received error signal for " + o, throwable))
//                .subscribe(consumer);
//    }
//
//}
