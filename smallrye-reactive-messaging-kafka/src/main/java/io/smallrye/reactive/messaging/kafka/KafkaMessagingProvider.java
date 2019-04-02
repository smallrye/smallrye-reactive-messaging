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
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@ApplicationScoped
public class KafkaMessagingProvider implements IncomingConnectorFactory, OutgoingConnectorFactory {

  @Inject
  private Vertx vertx;

  private List<KafkaSource> sources = new CopyOnWriteArrayList<>();
  private List<KafkaSink> sinks = new CopyOnWriteArrayList<>();

  public void terminate(@Observes @BeforeDestroyed(ApplicationScoped.class) Object event) {
    sources.forEach(KafkaSource::close);
    sinks.forEach(KafkaSink::close);
  }

  @Override
  public Class<? extends MessagingProvider> type() {
    return Kafka.class;
  }

  @Override
  public PublisherBuilder<KafkaMessage> getPublisherBuilder(Config config) {
    KafkaSource<Object, Object> source = new KafkaSource<>(vertx, config);
    sources.add(source);
    return source.getSource();
  }

  @Override
  public SubscriberBuilder<? extends Message, Void> getSubscriberBuilder(Config config) {
    KafkaSink sink = new KafkaSink(vertx, config);
    sinks.add(sink);
    return sink.getSink();
  }
}
