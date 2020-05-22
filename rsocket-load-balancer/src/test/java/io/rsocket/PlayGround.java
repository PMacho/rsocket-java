package io.rsocket;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class PlayGround {

  @Test
  public void contextTest() {
    Flux.range(0, 10)
        .flatMap(
            integer ->
                Mono.subscriberContext()
                    .map(
                        context -> {
                          Integer i = context.<Integer>get("index");
                          i++;
                          return i;
                        })
                    .map(index -> index + integer))
        .subscriberContext(context -> context.put("index", 2))
        .subscribe(System.out::println);

    try {
      Thread.sleep(200);
    } catch (Throwable t) {
    }
  }
}
