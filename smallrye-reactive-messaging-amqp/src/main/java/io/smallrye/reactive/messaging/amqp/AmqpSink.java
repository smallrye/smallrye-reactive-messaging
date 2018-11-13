package io.smallrye.reactive.messaging.amqp;

import io.vertx.core.json.JsonObject;
import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Subscriber;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

public class AmqpSink implements Closeable {

  private final ProtonSender sender;
  private final AtomicBoolean open = new AtomicBoolean();
  private final JsonObject config;
  private Subscriber<? extends Message> subscriber;

  AmqpSink(ProtonConnection connection, String address, JsonObject config) {
    this.config = config;
    sender = connection.createSender(address, new ProtonLinkOptions(config));
  }

  public CompletionStage<AmqpSink> init() {
    CompletableFuture<AmqpSink> future = new CompletableFuture<>();
    sender.openHandler(x -> {
      if (x.succeeded()) {
        open.set(true);
        future.complete(this);
      } else {
        open.set(false);
        future.completeExceptionally(x.cause());
      }
    });

    boolean durable = Boolean.parseBoolean(config.getString("durable", "true"));
    long ttl = Long.parseLong(config.getString("ttl", "0"));
    subscriber = ReactiveStreams.<Message>builder()
      .flatMapCompletionStage(msg -> {
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
            amqp.setTtl(ttl);
          }

          sender.send(amqp, delivered::complete);
        } catch (Exception e) {
          delivered.completeExceptionally(e);
        }
        return delivered;
      })
      .ignore()
      .build();

    sender.open();
    return future;
  }

  Subscriber<? extends Message> subscriber() {
    return subscriber;
  }

  boolean isOpen() {
    return open.get();
  }

  @Override
  public void close() {
    open.set(false);
    sender.close();
  }
}
