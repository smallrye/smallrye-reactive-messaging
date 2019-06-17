package io.smallrye.reactive.messaging.kafka;

import io.vertx.reactivex.core.Vertx;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@ApplicationScoped
@Connector(KafkaConnector.CONNECTOR_NAME)
public class KafkaConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

  public static final String CONNECTOR_NAME = "smallrye-kafka";
  @Inject
  private Instance<Vertx> instanceOfVertx;

  @Inject
  @ConfigProperty(name = "kafka.bootstrap.servers", defaultValue = "localhost:9092")
  private Instance<String> servers;

  private List<KafkaSource> sources = new CopyOnWriteArrayList<>();
  private List<KafkaSink> sinks = new CopyOnWriteArrayList<>();

  private boolean internalVertxInstance = false;
  private Vertx vertx;

  public void terminate(@Observes @BeforeDestroyed(ApplicationScoped.class) Object event) {
    sources.forEach(KafkaSource::close);
    sinks.forEach(KafkaSink::close);

    if (internalVertxInstance) {
      vertx.close();
    }
  }

  @PostConstruct
  void init() {
    if (instanceOfVertx.isUnsatisfied()) {
      internalVertxInstance = true;
      this.vertx = Vertx.vertx();
    } else {
      this.vertx = instanceOfVertx.get();
    }
  }

  @Override
  public PublisherBuilder<KafkaMessage> getPublisherBuilder(Config config) {
    String s = servers.isUnsatisfied() ? null : servers.get();
    KafkaSource<Object, Object> source = new KafkaSource<>(vertx, config, s);
    sources.add(source);
    return source.getSource();
  }

  @Override
  public SubscriberBuilder<? extends Message, Void> getSubscriberBuilder(Config config) {
    String s = servers.isUnsatisfied() ? null : servers.get();
    KafkaSink sink = new KafkaSink(vertx, config, s);
    sinks.add(sink);
    return sink.getSink();
  }
}
