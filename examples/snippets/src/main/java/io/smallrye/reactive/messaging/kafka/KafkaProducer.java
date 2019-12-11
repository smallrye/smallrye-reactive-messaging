package io.smallrye.reactive.messaging.kafka;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

public class KafkaProducer {

  // tag::kafka-message[]
  @Outgoing("to-kafka")
  public Message<String> produce(Message<String> incoming) {
    return KafkaMessage.of("topic", "key", incoming.getPayload());
  }
  // end::kafka-message[]

}
