package io.smallrye.reactive.messaging.eventbus;

import io.vertx.reactivex.core.Vertx;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class ConsumptionBean {

  private List<Integer> list = new ArrayList<>();

  @Incoming("data")
  @Outgoing("sink")
  public Message<Integer> process(EventBusMessage<Integer> input) {
    return Message.of(input.getPayload() + 1);
  }

  @Incoming("sink")
  public void sink(int val) {
    list.add(val);
  }

  @Produces
  public Config myConfig() {
    String prefix = "smallrye.messaging.source.data.";
    Map<String, String> config = new HashMap<>();
    config.put(prefix + "address", "data");
    config.put(prefix + "type", VertxEventBus.class.getName());
    return new MyConfig(config);
  }

  List<Integer> getResults() {
    return list;
  }

  @Inject
  Vertx vertx;

  public void produce() {
    AtomicInteger counter = new AtomicInteger();
    new Thread(() ->
      new EventBusUsage(vertx.eventBus().getDelegate())
        .produceIntegers("data", 10, true, null, counter::getAndIncrement))
      .start();
  }
}
