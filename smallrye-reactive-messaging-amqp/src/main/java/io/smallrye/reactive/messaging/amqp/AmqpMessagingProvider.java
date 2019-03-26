package io.smallrye.reactive.messaging.amqp;

import io.reactivex.Flowable;
import io.reactivex.processors.UnicastProcessor;
import io.smallrye.reactive.messaging.spi.ConfigurationHelper;
import io.smallrye.reactive.messaging.spi.IncomingConnectorFactory;
import io.smallrye.reactive.messaging.spi.OutgoingConnectorFactory;
import io.vertx.proton.*;
import io.vertx.reactivex.core.Context;
import io.vertx.reactivex.core.Vertx;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.MessagingProvider;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class AmqpMessagingProvider implements IncomingConnectorFactory, OutgoingConnectorFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpMessagingProvider.class);

  private final ProtonClient client;
  private final Context context;
  private List<Closeable> closeable = new ArrayList<>();

  /**
   * Keeps track of the receiver flows, to avoid recreating proton receiver and connections.
   */
  private final Map<String, Flowable<? extends Message>> receivers = new ConcurrentHashMap<>();

  @Inject
  AmqpMessagingProvider(Vertx vertx) {
    this.client = ProtonClient.create(vertx.getDelegate());
    this.context = vertx.getOrCreateContext();
  }

  @Override
  public Class<? extends MessagingProvider> type() {
    return Amqp.class;
  }


  private CompletionStage<ProtonConnection> connect(Config config) {
    CompletableFuture<ProtonConnection> future = new CompletableFuture<>();
    String username = config.getOptionalValue("username", String.class).orElse(null);
    String password = config.getOptionalValue("password", String.class).orElse(null);
    String host = config.getOptionalValue("host", String.class)
      .orElseThrow(() -> new IllegalArgumentException("Host must be set"));
    int port = config.getOptionalValue("port", Integer.class).orElse(5672);
    String containerId = config.getOptionalValue("containerId", String.class).orElse(null);

    this.context.runOnContext(ignored -> {
      ProtonClientOptions options = new ProtonClientOptions(ConfigurationHelper.create(config).asJsonObject());
      if (options.getReconnectAttempts() <= 0) {
        options.setReconnectAttempts(100).setReconnectInterval(10).setConnectTimeout(1000);
      }

      client.connect(options, host, port, username, password, ar -> {
        if (ar.failed()) {
          future.completeExceptionally(ar.cause());
        } else {
          ProtonConnection connection = ar.result().setContainer(containerId);
          connection.openHandler(x -> {
            if (x.succeeded()) {
              closeable.add(connection::close);
              future.complete(x.result());
            } else {
              future.completeExceptionally(x.cause());
            }
          });
          connection.open();
        }
      });
    });
    return future;
  }

  @Override
  public PublisherBuilder<? extends Message> getPublisherBuilder(Config config) {
    String address = config.getOptionalValue("address", String.class)
      .orElseThrow(() -> new IllegalArgumentException("Address must be set"));
    boolean broadcast = config.getOptionalValue("broadcast", Boolean.class).orElse(false);

    String name = config.getValue("name", String.class);

    return ReactiveStreams.fromCompletionStage(connect(config).thenCompose(
      c -> {
        CompletableFuture<ProtonReceiver> future = new CompletableFuture<>();
        ProtonReceiver receiver = c.createReceiver(address);
        receiver.openHandler(x -> {
          if (x.succeeded()) {
            closeable.add(receiver::close);
            future.complete(receiver);
          } else {
            future.completeExceptionally(x.cause());
          }
        });
        receiver.open();
        return future;
      }
    )).flatMapRsPublisher(receiver -> {
      Flowable<? extends Message> existing = receivers.get(name);
      if (existing != null) {
        return existing;
      } else {
        UnicastProcessor<Message> processor = UnicastProcessor.create();
        receiver.handler(((delivery, message) -> processor.onNext(new AmqpMessage(delivery, message))));
        existing = processor
          .compose(flow -> broadcast ? flow.publish().autoConnect() : flow
          );
        receivers.put(name, existing);
        return existing;
      }
    });
  }

  @Override
  public SubscriberBuilder<? extends Message, Void> getSubscriberBuilder(Config config) {
    String address = config.getOptionalValue("address", String.class)
      .orElseThrow(() -> new IllegalArgumentException("Address must be set"));
    boolean durable = config.getOptionalValue("durable", Boolean.class).orElse(true);
    long ttl = config.getOptionalValue("ttl", Long.class).orElse(0L);

    AtomicReference<ProtonSender> sender = new AtomicReference<>();

    return ReactiveStreams.<Message>builder().flatMapCompletionStage(message -> {
      ProtonSender actualProtonSender = sender.get();
      if (actualProtonSender == null) {
        return connect(config)
          .thenApply(connection -> connection.createSender(address))
          .thenCompose(ps -> {
            CompletableFuture<ProtonSender> future = new CompletableFuture<>();
            ps.openHandler(x -> {
              if (x.failed()) {
                future.completeExceptionally(x.cause());
              } else {
                sender.set(ps);
                closeable.add(ps::close);
                future.complete(ps);
              }
            });
            ps.open();
            return future;
          }).thenCompose(ps -> send(ps, message, durable, ttl));
      } else {
        return send(actualProtonSender, message, durable, ttl);
      }
    }).ignore();
  }

  private CompletionStage<ProtonDelivery> send(ProtonSender sender, Message msg, boolean durable, long ttl) {
    LOGGER.info("Sending... {}", msg.getPayload());
    CompletableFuture<ProtonDelivery> delivered = new CompletableFuture<>();
    try {
      org.apache.qpid.proton.message.Message amqp;

      if (msg instanceof AmqpMessage) {
        amqp = ((AmqpMessage) msg).unwrap();
      } else if (msg.getPayload() instanceof org.apache.qpid.proton.message.Message) {
        amqp = (org.apache.qpid.proton.message.Message) msg.getPayload();
      } else {
        amqp = ProtonHelper.message();
        amqp.setBody(new AmqpValue(msg.getPayload()));
        amqp.setDurable(durable);
        if (ttl != 0) {
          amqp.setTtl(ttl);
        }
      }
      sender.send(amqp, delivery -> {
        if (delivery.getRemoteState().getType() == DeliveryState.DeliveryStateType.Accepted) {
          LOGGER.info("Message accepted by the AMQP broker");
          delivered.complete(delivery);
        } else {
          LOGGER.info("Message rejected with delivery: {}", delivery.getRemoteState().getType());
          delivered.completeExceptionally(new Exception("Unable to deliver message: " + delivery.getRemoteState().getType()));
        }
      });
    } catch (Exception e) {
      delivered.completeExceptionally(e);
    }
    return delivered;
  }

  @PreDestroy
  public synchronized void close() {
    // Reverse to get sender and receiver before the connection
    Collections.reverse(closeable);
    closeable.forEach(toBeClose -> {
      try {
        toBeClose.close();
      } catch (IOException e) {
        // Ignored.
      }
    });
    closeable.clear();
    receivers.clear();
  }
}
