package io.smallrye.reactive.messaging.amqp;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.processors.UnicastProcessor;
import io.smallrye.reactive.messaging.spi.ConfigurationHelper;
import io.smallrye.reactive.messaging.spi.PublisherFactory;
import io.smallrye.reactive.messaging.spi.SubscriberFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.proton.*;
import io.vertx.reactivex.core.Vertx;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.MessagingProvider;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
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

    CompletableFuture<ProtonConnection> future = new CompletableFuture<>();
    client.connect(options, host, port, username, password, ar -> {

      if (ar.failed()) {
        future.completeExceptionally(ar.cause());
      } else {
        try {
          connection = ar.result().setContainer(config.get("containerId"));
          connection.openHandler(x -> future.complete(x.result())).open();
        } catch (Exception e) {
          future.completeExceptionally(e);
        }
      }
    });
    return future;
  }

  @Override

  public CompletionStage<Subscriber<? extends Message>> createSubscriber(Map<String, String> config) {
    ConfigurationHelper conf = ConfigurationHelper.create(config);
    String address = conf.getOrDie("address");

    return connect(conf)
      .thenApply(conn -> {
        ProtonSender sender = conn.createSender(address, new ProtonLinkOptions(conf.asJsonObject())).open();

        return ReactiveStreams.<Message>builder().flatMapCompletionStage(msg -> {
          CompletableFuture<ProtonDelivery> future = new CompletableFuture<>();
          org.apache.qpid.proton.message.Message amqp;

          if (msg instanceof AmqpMessage) {
            amqp = ((AmqpMessage) msg).unwrap();
          } else if (msg.getPayload() instanceof org.apache.qpid.proton.message.Message) {
            amqp = (org.apache.qpid.proton.message.Message) msg.getPayload();
          } else {
            amqp = ProtonHelper.message();
            amqp.setBody(new AmqpValue(msg.getPayload()));
            amqp.setDurable(conf.getAsBoolean("durable", true));
            amqp.setTtl(conf.getAsLong("ttl").orElse(0L));
          }

          sender.send(amqp, future::complete);
          closeable.add(sender::close);
          return future;
        })
          .ignore().build();
      });
  }

  @Override
  public CompletionStage<Publisher<? extends Message>> createPublisher(Map<String, String> config) {
    ConfigurationHelper conf = ConfigurationHelper.create(config);
    String address = conf.getOrDie("address");
    boolean multicast = conf.getAsBoolean("multicast", false);
    JsonObject copy = conf.asJsonObject();

    return connect(conf)
      .thenApply(conn -> {
        // TODO Rewrite it with the Proton RS support coming in Vert.x 3.6
        ProtonReceiver receiver = conn.createReceiver(address, new ProtonLinkOptions(copy));
        UnicastProcessor<Message> processor = UnicastProcessor.create();
        receiver.handler(((delivery, message) -> processor.onNext(new AmqpMessage(delivery, message))));

        return Flowable.defer(() -> {
          CompletableFuture<Void> opened = new CompletableFuture<>();
          Publisher<Void> hasBeenOpened = Completable
            .create(emitter -> opened.whenComplete((s, e) -> {
              if (e == null) {
                emitter.onComplete();
              } else {
                emitter.onError(e);
              }
            }))
            .toFlowable();

          receiver.openHandler(ar -> {
            if (ar.failed()) {
              opened.completeExceptionally(ar.cause());
            } else {
              opened.complete(null);
            }
          });

          receiver.open();
          closeable.add(receiver::close);
          return processor.delaySubscription(hasBeenOpened);
        })
          .compose(f -> {
            if (multicast) {
              return f.publish().autoConnect();
            } else {
              return f;
            }
          });
      });
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

    if (connection != null && connection.isDisconnected()) {
      connection.close();
      connection.disconnect();
      connection = null;
    }
  }
}
