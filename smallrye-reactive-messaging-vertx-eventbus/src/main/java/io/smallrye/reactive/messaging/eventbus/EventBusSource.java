package io.smallrye.reactive.messaging.eventbus;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.smallrye.reactive.messaging.spi.ConfigurationHelper;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.eventbus.MessageConsumer;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class EventBusSource {


  private final String address;
  private final boolean ack;
  private final Vertx vertx;
  private final boolean multicast;

  public EventBusSource(Vertx vertx, ConfigurationHelper config) {
    this.vertx = Objects.requireNonNull(vertx,"The vert.x instance must not be `null`");
    this.address = config.getOrDie("address");
    this.multicast = config.getAsBoolean("multicast", false);
    this.ack = config.getAsBoolean("use-reply-as-ack", false);
  }

  public Publisher<? extends Message> publisher() {
    MessageConsumer<Message> consumer = vertx.eventBus().consumer(address);
    return consumer.toFlowable()
      .compose(flow -> {
        if (multicast) {
          return flow.publish().autoConnect();
        } else {
          return flow;
        }
      })
      .map(this::adapt);
  }

  private Message adapt(io.vertx.reactivex.core.eventbus.Message msg) {
    if (this.ack) {
      return new EventBusMessage<>(msg, () -> {
        msg.reply("OK"); // TODO Should we return something in particular?
        return CompletableFuture.completedFuture(null);
      });
    } else {
      return new EventBusMessage<>(msg, null);
    }
  }
}

