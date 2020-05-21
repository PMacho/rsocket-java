//package io.rsocket.client.new_classes;
//
//import io.rsocket.AbstractRSocket;
//import io.rsocket.Payload;
//import io.rsocket.RSocket;
//import io.rsocket.addons.strategies.weighted.pool.WeightedRSocketPool;
//import org.reactivestreams.Publisher;
//import reactor.core.publisher.Flux;
//import reactor.core.publisher.Mono;
//
//import java.util.Collection;
//
//public class LoadBalancedRSocket extends AbstractRSocket {
//
//    private final RSocketPool<? extends RSocket> rSocketPool;
//
//    public <S extends RSocket> LoadBalancedRSocket(
//            Publisher<? extends Collection<RSocket>> rSocketsPublisher,
//            RSocketPool<S> rSocketPool
//    ) {
//        this.rSocketPool = rSocketPool;
//        this.rSocketPool.start(rSocketsPublisher);
//    }
//
//    public LoadBalancedRSocket(Publisher<? extends Collection<RSocket>> rSocketsPublisher) {
//        this(rSocketsPublisher, new WeightedRSocketPool());
//    }
//
//    private Mono<? extends RSocket> select() {
//        return rSocketPool.select();
//    }
//
//    @Override
//    public Mono<Void> fireAndForget(Payload payload) {
//        return select()
//                .flatMap(rSocket -> rSocket.fireAndForget(payload))
//                .doOnNext(n -> rSocketPool.update());
//    }
//
//    @Override
//    public Mono<Payload> requestResponse(Payload payload) {
//        return select()
//                .flatMap(rSocket -> rSocket.requestResponse(payload))
//                .doOnNext(n -> rSocketPool.update());
//    }
//
//    @Override
//    public Flux<Payload> requestStream(Payload payload) {
//        return select()
//                .flatMapMany(rSocket -> rSocket.requestStream(payload))
//                .doOnComplete(rSocketPool::update);
//    }
//
//    @Override
//    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
//        return select()
//                .flatMapMany(rSocket -> rSocket.requestChannel(payloads))
//                .doOnComplete(rSocketPool::update);
//    }
//
//    @Override
//    // fixme: Am I really correct?
//    public Mono<Void> metadataPush(Payload payload) {
//        return rSocketPool
//                .selectAll()
//                .flatMap(rSocket -> rSocket.metadataPush(payload))
//                .doOnComplete(rSocketPool::update)
//                .then();
//    }
//
//    @Override
//    public void dispose() {
//        rSocketPool
//                .clean()
//                .then(Mono.fromRunnable(super::dispose))
//                .subscribe();
//    }
//
//}
