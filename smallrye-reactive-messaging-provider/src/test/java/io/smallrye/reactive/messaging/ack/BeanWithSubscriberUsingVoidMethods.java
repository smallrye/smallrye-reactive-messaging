package io.smallrye.reactive.messaging.ack;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import javax.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * It is not supported to return `void` or an object (that it not a CompletionStage) when consuming a Message. So, we are checking method returning
 * `CompletionStage<Void>`.
 */
@ApplicationScoped
public class BeanWithSubscriberUsingVoidMethods {

  public static final String MANUAL_ACKNOWLEDGMENT = "manual-acknowledgment";
  public static final String NO_ACKNOWLEDGMENT = "no-acknowledgment";
  public static final String AUTO_ACKNOWLEDGMENT = "auto-acknowledgment";
  private Map<String, List<String>> sink = new ConcurrentHashMap<>();
  private Map<String, List<String>> acknowledged = new ConcurrentHashMap<>();

  public List<String> received(String key) {
    return sink.get(key);
  }

  public List<String> acknowledged(String key) {
    return acknowledged.get(key);
  }

  @Incoming(MANUAL_ACKNOWLEDGMENT)
  @Acknowledgment(Acknowledgment.Mode.MANUAL)
  public CompletionStage<Void> subWithAck(Message<String> message) {
    this.sink.computeIfAbsent(MANUAL_ACKNOWLEDGMENT, x -> new CopyOnWriteArrayList<>()).add(message.getPayload());
    // We cannot return a value or void by spec
    // So we return the CompletionStage<Void>.
    return message.ack();
  }

  @Outgoing(MANUAL_ACKNOWLEDGMENT)
  public Publisher<Message<String>> sourceToManualAck() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> CompletableFuture.runAsync(() -> {
          nap();
          acknowledged.computeIfAbsent(MANUAL_ACKNOWLEDGMENT, x -> new CopyOnWriteArrayList<>()).add(payload);
        }))
      );
  }

  @Incoming(NO_ACKNOWLEDGMENT)
  @Acknowledgment(Acknowledgment.Mode.NONE)
  public CompletionStage<Void> subWithNoAck(Message<String> message) {
    this.sink.computeIfAbsent(NO_ACKNOWLEDGMENT, x -> new CopyOnWriteArrayList<>()).add(message.getPayload());
    // We cannot return a value or void by spec
    // So we return the CompletionStage<Void>.
    return CompletableFuture.completedFuture(null);
  }

  @Outgoing(NO_ACKNOWLEDGMENT)
  public Publisher<Message<String>> sourceToNoAck() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> {
        nap();
        acknowledged.computeIfAbsent(NO_ACKNOWLEDGMENT, x -> new CopyOnWriteArrayList<>()).add(payload);
        return CompletableFuture.completedFuture(null);
      }));
  }

  // TODO Post, PRe and Default

  @Incoming(AUTO_ACKNOWLEDGMENT)
  public void subWithAutoAck(String payload) {
    sink.computeIfAbsent(AUTO_ACKNOWLEDGMENT, x -> new CopyOnWriteArrayList<>()).add(payload);
  }

  @Outgoing(AUTO_ACKNOWLEDGMENT)
  public Publisher<Message<String>> sourceToAutoAck() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> {
        nap();
        acknowledged.computeIfAbsent(AUTO_ACKNOWLEDGMENT, x -> new CopyOnWriteArrayList<>()).add(payload);
        return CompletableFuture.completedFuture(null);
      }));
  }

  private void nap() {
    try {
      Thread.sleep(10);
    } catch (Exception e) {
      // Ignore me.
    }
  }

}
