package io.smallrye.reactive.messaging.http;

import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

public class HttpExample {

  // tag::http-message[]
  @Incoming("source")
  @Outgoing("to-http")
  public HttpMessage<JsonObject> consume(Message<String> incoming) {
    return HttpMessage.HttpMessageBuilder.<JsonObject>create()
      .withMethod("PUT")
      .withPayload(new JsonObject().put("value", incoming.getPayload().toUpperCase()))
      .withHeader("Content-Type", "application/json")
      .build();
  }
  // end::http-message[]
}
