package io.smallrye.reactive.messaging.amqp;

import io.reactivex.Flowable;
import io.reactivex.processors.UnicastProcessor;
import io.vertx.core.json.JsonObject;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonLinkOptions;
import io.vertx.proton.ProtonReceiver;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class AmqpSource implements Closeable {

  private final ProtonReceiver receiver;
  private final boolean multicast;
  private AtomicBoolean open = new AtomicBoolean();
  private Flowable<? extends Message> publisher;


  AmqpSource(ProtonConnection connection, String address, boolean multicast, JsonObject copy) {
    this.multicast = multicast;
    this.receiver = connection.createReceiver(address, new ProtonLinkOptions(copy));
  }

  CompletableFuture<AmqpSource> init() {
    CompletableFuture<AmqpSource> future = new CompletableFuture<>();
    this.receiver.openHandler(x -> {
      if (x.succeeded()) {
        open.set(true);
        future.complete(this);
      } else {
        open.set(false);
        future.completeExceptionally(x.cause());
      }
    });

    UnicastProcessor<Message> processor = UnicastProcessor.create();
    receiver.handler(((delivery, message) -> processor.onNext(new AmqpMessage(delivery, message))));

    publisher = Flowable.fromPublisher(processor).compose(
      flow -> multicast ? flow.publish().autoConnect() : flow
    );
    receiver.open();
    return future;
  }

  Publisher<? extends Message> publisher() {
    return publisher;
  }

  public boolean isOpen() {
    return open.get();
  }

  @Override
  public void close() {
    receiver.close();
    open.set(false);
  }


}
