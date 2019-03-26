package io.smallrye.reactive.messaging.camel.source;

import io.smallrye.reactive.messaging.camel.Camel;
import io.smallrye.reactive.messaging.camel.CamelMessage;
import io.smallrye.reactive.messaging.camel.MapBasedConfig;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class BeanWithCamelSourceUsingRegularEndpoint {

  private List<String> list = new ArrayList<>();

  @Incoming("data")
  public CompletionStage<Void> sink(CamelMessage<String> msg) {
    list.add(msg.getPayload());
    return msg.ack();
  }

  public List<String> list() {
    return list;
  }

  @Produces
  public Config myConfig() {
    String prefix = "mp.messaging.provider.incoming.data.";
    Map<String, Object> config = new HashMap<>();
    config.putIfAbsent(prefix +  "endpoint-uri", "seda:out");
    config.put(prefix + "type", Camel.class.getName());
    return new MapBasedConfig(config);
  }


}
