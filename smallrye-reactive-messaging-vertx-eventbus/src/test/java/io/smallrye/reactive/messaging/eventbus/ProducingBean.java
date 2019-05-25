package io.smallrye.reactive.messaging.eventbus;

import io.reactivex.Flowable;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import io.vertx.reactivex.core.Vertx;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class ProducingBean {

  @Incoming("data")
  @Outgoing("sink")
  @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
  public Message<Integer> process(Message<Integer> input) {
    return Message.of(input.getPayload() + 1);
  }

  @Outgoing("data")
  public Publisher<Integer> source() {
    return Flowable.range(0, 10);
  }

  @Produces
  public Config myConfig() {
    String prefix = "mp.messaging.outgoing.sink.";
    Map<String, Object> config = new HashMap<>();
    config.put(prefix + "address", "sink");
    config.put(prefix + "connector", VertxEventBusConnector.CONNECTOR_NAME);
    return new MapBasedConfig(config);
  }

  // As we can't use the Usage class - not the same Vert.x instance, receive the message here.

  @Inject
  Vertx vertx;

  private List<io.vertx.reactivex.core.eventbus.Message> messages = new ArrayList<>();


  @PostConstruct
  public void registerConsumer() {
    vertx.eventBus().consumer("sink").handler(m -> messages.add(m));
  }

  public List<io.vertx.reactivex.core.eventbus.Message> messages() {
    return messages;
  }

}
