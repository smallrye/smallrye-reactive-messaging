package io.smallrye.reactive.messaging.mqtt;

import io.smallrye.reactive.messaging.spi.PublisherFactory;
import io.smallrye.reactive.messaging.spi.SubscriberFactory;
import io.vertx.reactivex.core.Vertx;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.MessagingProvider;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class MqttMessagingProvider  implements PublisherFactory, SubscriberFactory {
  @Override
  public Class<? extends MessagingProvider> type() {
    return Mqtt.class;
  }

  @Override
  public CompletionStage<Subscriber<? extends Message>> createSubscriber(Vertx vertx, Map<String, String> config) {
    return new MqttSink(vertx, config).initialize();
  }

  @Override
  public CompletionStage<Publisher<? extends Message>> createPublisher(Vertx vertx, Map<String, String> config) {
    return new MqttSource(vertx, config).initialize();
  }
}
