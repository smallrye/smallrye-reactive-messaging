package io.smallrye.reactive.messaging.http;

import io.vertx.reactivex.core.Vertx;
import org.eclipse.microprofile.config.Config;
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

@ApplicationScoped
@Connector(HttpConnector.CONNECTOR_NAME)
public class HttpConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

  public static final String CONNECTOR_NAME = "smallrye-http";
  @Inject
  private Instance<Vertx> instanceOfVertx;

  private boolean internalVertxInstance = false;
  private Vertx vertx;

  public void terminate(@Observes @BeforeDestroyed(ApplicationScoped.class) Object event) {
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
  public SubscriberBuilder<? extends Message, Void> getSubscriberBuilder(Config config) {
    return new HttpSink(vertx, config).sink();
  }

  @Override
  public PublisherBuilder<? extends Message> getPublisherBuilder(Config config) {
    return new HttpSource(vertx, config).source();
  }


}
