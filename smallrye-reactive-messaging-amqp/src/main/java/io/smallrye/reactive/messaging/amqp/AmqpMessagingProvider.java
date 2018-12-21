package io.smallrye.reactive.messaging.amqp;

import io.smallrye.reactive.messaging.spi.ConfigurationHelper;
import io.smallrye.reactive.messaging.spi.PublisherFactory;
import io.smallrye.reactive.messaging.spi.SubscriberFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonClientOptions;
import io.vertx.proton.ProtonConnection;
import io.vertx.reactivex.core.Vertx;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.MessagingProvider;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class AmqpMessagingProvider implements PublisherFactory, SubscriberFactory {

  private Vertx vertx;
  private ProtonClient client;
  private ProtonConnection connection;
  private List<Closeable> closeable = new ArrayList<>();

  @Inject
  AmqpMessagingProvider(Vertx vertx) {
    this.vertx = vertx;
  }

  @PostConstruct
  public void configure() {
    client = ProtonClient.create(vertx.getDelegate());
  }

  @Override
  public Class<? extends MessagingProvider> type() {
    return Amqp.class;
  }


  private CompletionStage<ProtonConnection> connect(ConfigurationHelper config) {
    String username = config.get("username");
    String password = config.get("password");
    String host = config.getOrDie("host");
    int port = config.getAsInteger("port", 5672);
    ProtonClientOptions options = new ProtonClientOptions(config.asJsonObject());
    if (options.getReconnectAttempts() <= 0){
      options.setReconnectAttempts(100).setReconnectInterval(10).setConnectTimeout(1000);
    }

    CompletableFuture<ProtonConnection> future = new CompletableFuture<>();
    client.connect(options, host, port, username, password, ar -> {
      if (ar.failed()) {
        future.completeExceptionally(ar.cause());
      } else {
        connection = ar.result().setContainer(config.get("containerId"));
        connection.openHandler(x -> {
          if (x.succeeded()) {
            future.complete(x.result());
          } else {
            future.completeExceptionally(x.cause());
          }
        });
        connection.open();
      }
    });
    return future;
  }

  @Override
  public CompletionStage<Subscriber<? extends Message>> createSubscriber(Map<String, String> config) {
    return getSink(config)
      .thenApply(AmqpSink::subscriber);
  }

  @Override
  public CompletionStage<Publisher<? extends Message>> createPublisher(Map<String, String> config) {
    return getSource(config)
      .thenApply(AmqpSource::publisher);
  }

  CompletionStage<AmqpSource> getSource(Map<String, String> config) {
    ConfigurationHelper conf = ConfigurationHelper.create(config);
    String address = conf.getOrDie("address");
    boolean broadcast = conf.getAsBoolean("broadcast", false);
    JsonObject copy = conf.asJsonObject();

    return connect(conf)
      .thenApply(conn -> {
        AmqpSource source = new AmqpSource(conn, address, broadcast, copy);
        closeable.add(source);
        return source;
      })
      .thenCompose(AmqpSource::init);
  }

  CompletionStage<AmqpSink> getSink(Map<String, String> config) {
    ConfigurationHelper conf = ConfigurationHelper.create(config);
    String address = conf.getOrDie("address");
    JsonObject copy = conf.asJsonObject();

    return connect(conf)
      .thenApply(conn -> {
        AmqpSink sink = new AmqpSink(conn, address, copy);
        closeable.add(sink);
        return sink;
      })
      .thenCompose(AmqpSink::init);
  }

  @PreDestroy
  public synchronized void close() {
    closeable.forEach(toBeClose -> {
      try {
        toBeClose.close();
      } catch (IOException e) {
        // Ignored.
      }
    });
    closeable.clear();

    if (connection != null) {
      connection.close();
      connection = null;
    }
  }
}
