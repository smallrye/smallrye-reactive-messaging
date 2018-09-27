package io.smallrye.reactive.messaging.kafka;

import io.smallrye.reactive.messaging.spi.PublisherFactory;
import io.smallrye.reactive.messaging.spi.SubscriberFactory;
import io.vertx.reactivex.core.Vertx;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.MessagingProvider;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Map;
import java.util.concurrent.CompletionStage;

public class KafkaMessagingProvider implements PublisherFactory, SubscriberFactory {
  @Override
  public Class<? extends MessagingProvider> type() {
    return Kafka.class;
  }

  @Override
  public CompletionStage<Subscriber<? extends Message>> createSubscriber(Vertx vertx, Map<String, String> config) {
    return KafkaSink.create(vertx, config);
  }

  @Override
  public CompletionStage<Publisher<? extends Message>> createPublisher(Vertx vertx, Map<String, String> config) {
    return KafkaSource.create(vertx, config);
  }
}
