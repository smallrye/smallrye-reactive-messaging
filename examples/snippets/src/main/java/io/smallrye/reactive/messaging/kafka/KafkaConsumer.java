package io.smallrye.reactive.messaging.kafka;

import io.vertx.core.json.JsonObject;
import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.reactive.messaging.Incoming;

public class KafkaConsumer {

  // tag::kafka-message[]
  @Incoming("from-kafka")
  public void consume(KafkaRecord<String, JsonObject> message) {
    JsonObject payload = message.getPayload();
    String key = message.getKey();
    Headers headers = message.getHeaders();
    int partition = message.getPartition();
    long timestamp = message.getTimestamp();
  }
  // end::kafka-message[]

}
