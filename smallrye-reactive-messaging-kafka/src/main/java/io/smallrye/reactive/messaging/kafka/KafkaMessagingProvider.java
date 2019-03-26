package io.smallrye.reactive.messaging.kafka;

import io.smallrye.reactive.messaging.spi.IncomingConnectorFactory;
import io.smallrye.reactive.messaging.spi.OutgoingConnectorFactory;
import io.vertx.reactivex.core.Vertx;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.MessagingProvider;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class KafkaMessagingProvider implements IncomingConnectorFactory, OutgoingConnectorFactory {

  @Inject
  private Vertx vertx;

  @Override
  public Class<? extends MessagingProvider> type() {
    return Kafka.class;
  }

  @Override
  public PublisherBuilder<KafkaMessage> getPublisherBuilder(Config config) {
    return new KafkaSource<>(vertx, config).getSource();
  }

  @Override
  public SubscriberBuilder<? extends Message, Void> getSubscriberBuilder(Config config) {
    return new KafkaSink(vertx, config).getSink();
  }
}
