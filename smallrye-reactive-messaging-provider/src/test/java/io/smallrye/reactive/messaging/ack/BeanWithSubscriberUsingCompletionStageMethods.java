package io.smallrye.reactive.messaging.ack;

import io.reactivex.Flowable;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import javax.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@ApplicationScoped
public class BeanWithSubscriberUsingCompletionStageMethods {

  public static final String MANUAL_ACKNOWLEDGMENT = "manual-acknowledgment";
  public static final String NO_ACKNOWLEDGMENT = "no-acknowledgment";
  public static final String AUTO_ACKNOWLEDGMENT = "auto-acknowledgment";
  private Map<String, List<String>> sink = new ConcurrentHashMap<>();
  private Map<String, List<String>> acknowledged = new ConcurrentHashMap<>();

  private Executor executor = Executors.newSingleThreadExecutor();

  public List<String> received(String key) {
    return sink.get(key);
  }

  public List<String> acknowledged(String key) {
    return acknowledged.get(key);
  }

  @Incoming(MANUAL_ACKNOWLEDGMENT)
  public CompletionStage<Void> subWithAck(Message<String> message) {
    return CompletableFuture.runAsync(() ->
      this.sink.computeIfAbsent(MANUAL_ACKNOWLEDGMENT, x -> new CopyOnWriteArrayList<>()).add(message.getPayload()), executor)
      .thenCompose(x -> message.ack());
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
  public CompletionStage<Void> subWithNoAck(Message<String> message) {
    this.sink.computeIfAbsent(NO_ACKNOWLEDGMENT, x -> new CopyOnWriteArrayList<>()).add(message.getPayload());
    return CompletableFuture.runAsync(this::nap);
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

  @Incoming(AUTO_ACKNOWLEDGMENT)
  public CompletionStage<Void> subWithAutoAck(String payload) {
    return CompletableFuture.runAsync(() ->
      sink.computeIfAbsent(AUTO_ACKNOWLEDGMENT, x -> new CopyOnWriteArrayList<>()).add(payload));
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
