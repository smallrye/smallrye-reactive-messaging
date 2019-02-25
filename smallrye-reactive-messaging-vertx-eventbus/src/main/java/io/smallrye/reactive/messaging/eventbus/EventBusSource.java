package io.smallrye.reactive.messaging.eventbus;

import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.eventbus.MessageConsumer;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

class EventBusSource {

  private final String address;
  private final boolean ack;
  private final Vertx vertx;
  private final boolean broadcast;

  EventBusSource(Vertx vertx, Config config) {
    this.vertx = Objects.requireNonNull(vertx, "The vert.x instance must not be `null`");
    this.address = config.getOptionalValue("address", String.class)
      .orElseThrow(() -> new IllegalArgumentException("`address` must be set"));
    this.broadcast = config.getOptionalValue("broadcast", Boolean.class).orElse(false);
    this.ack = config.getOptionalValue("use-reply-as-ack", Boolean.class).orElse(false);
  }

  PublisherBuilder<? extends Message> source() {
    MessageConsumer<Message> consumer = vertx.eventBus().consumer(address);
    return ReactiveStreams.fromPublisher(consumer.toFlowable()
      .compose(flow -> {
        if (broadcast) {
          return flow.publish().autoConnect();
        } else {
          return flow;
        }
      }))
      .map(this::adapt);
  }

  private Message adapt(io.vertx.reactivex.core.eventbus.Message msg) {
    if (this.ack) {
      return new EventBusMessage<>(msg, () -> {
        msg.reply("OK");
        return CompletableFuture.completedFuture(null);
      });
    } else {
      return new EventBusMessage<>(msg, null);
    }
  }
}

