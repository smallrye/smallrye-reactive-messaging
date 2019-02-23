package io.smallrye.reactive.messaging.camel.sink;

import io.smallrye.reactive.messaging.camel.Camel;
import io.smallrye.reactive.messaging.camel.MyConfig;
import org.apache.camel.builder.RouteBuilder;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.util.HashMap;
import java.util.Map;

@ApplicationScoped
public class BeanWithCamelSinkUsingRegularRoute extends RouteBuilder {

  @Outgoing("data")
  public Publisher<Message<String>> source() {
    return ReactiveStreams.of("a", "b", "c", "d")
      .map(String::toUpperCase)
      .map(Message::of)
      .buildRs();
  }

  @Produces
  public Config myConfig() {
    String prefix = "mp.messaging.provider.outgoing.data.";
    Map<String, String> config = new HashMap<>();
    config.putIfAbsent(prefix +  "endpoint-uri", "seda:in");
    config.put(prefix + "type", Camel.class.getName());
    return new MyConfig(config);
  }

  @Override
  public void configure() {
    from("seda:in").to("file:./target?fileName=values.txt&fileExist=append");
  }
}
