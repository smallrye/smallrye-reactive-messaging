package io.smallrye.reactive.messaging.eventbus;

import io.smallrye.reactive.messaging.spi.ConfigurationHelper;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.eventbus.MessageProducer;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.CompletionSubscriber;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Subscriber;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class EventBusSink {

  private final String address;
  private final Vertx vertx;
  private final boolean send;
  private final boolean expectReply;
  private final String codec;
  private final int timeout;

  public EventBusSink(Vertx vertx, ConfigurationHelper config) {
    this.vertx = Objects.requireNonNull(vertx, "Vert.x instance must not be `null`");
    this.address = config.getOrDie("address");
    this.send = config.getAsBoolean("publish", true);
    this.expectReply = config.getAsBoolean("expect-reply", false);

    if (! this.send && this.expectReply) {
      throw new IllegalArgumentException("Cannot disable `send` and enable `expectreply` at the same time");
    }

    this.codec = config.get("codec");
    this.timeout = config.getAsInteger("tiemout", -1);
  }


  public Subscriber<Message> subscriber() {
    DeliveryOptions options = new DeliveryOptions();
    if (this.codec != null) {
      options.setCodecName(this.codec);
    }
    if (this.timeout != -1) {
      options.setSendTimeout(this.timeout);
    }

    return ReactiveStreams.<Message>builder()
      .flatMapCompletionStage(msg -> {
        CompletableFuture<Message> future = new CompletableFuture<>();
        // TODO support getting an EventBusMessage as message.
        if (this.send) {
          if (expectReply) {
            vertx.eventBus().send(address, msg.getPayload(), ar -> {
              if (ar.failed()) {
                future.completeExceptionally(ar.cause());
              } else {
                future.complete(msg);
              }
            });
          } else {
            System.out.println("Sending " + msg.getPayload() + " to " + address + " / " + vertx);
            vertx.eventBus().send(address, msg.getPayload());
            future.complete(msg);
          }
        } else {
          vertx.eventBus().publish(address, msg.getPayload());
          future.complete(msg);
        }
        return future;
      })
      .ignore()
      .build();
  }


}
