package io.smallrye.reactive.messaging.ack;

import io.reactivex.Flowable;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Publisher;

import javax.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@ApplicationScoped
public class BeanWithProcessorsConsumingItems {

  static final String MANUAL_ACKNOWLEDGMENT = "manual-acknowledgment";
  static final String NO_ACKNOWLEDGMENT = "no-acknowledgment";
  static final String AUTO_ACKNOWLEDGMENT = "auto-acknowledgment";
  static final String MANUAL_ACKNOWLEDGMENT_CS = "manual-acknowledgment-cs";
  static final String NO_ACKNOWLEDGMENT_CS = "no-acknowledgment-cs";
  static final String AUTO_ACKNOWLEDGMENT_CS = "auto-acknowledgment-cs";

  private Map<String, List<String>> sink = new ConcurrentHashMap<>();
  private Map<String, List<String>> acknowledged = new ConcurrentHashMap<>();

  public List<String> received(String key) {
    return sink.get(key);
  }

  public List<String> acknowledged(String key) {
    return acknowledged.get(key);
  }

  // TODO a sink should be able to receive more than one mediator.

  @Incoming("sink-manual")
  public CompletionStage<Void> sinkManual(Message<String> ignored) {
    return CompletableFuture.completedFuture(null);
  }

  @Incoming("sink-auto")
  public CompletionStage<Void> sinkAuto(Message<String> ignored) {
    return CompletableFuture.completedFuture(null);
  }

  @Incoming("sink-no")
  public CompletionStage<Void> sinkNo(Message<String> ignored) {
    return CompletableFuture.completedFuture(null);
  }

  @Incoming("sink-manual-cs")
  public CompletionStage<Void> sinkManualForBuilder(Message<String> ignored) {
    return CompletableFuture.completedFuture(null);
  }

  @Incoming("sink-auto-cs")
  public CompletionStage<Void> sinkAutoForBuilder(Message<String> ignored) {
    return CompletableFuture.completedFuture(null);
  }

  @Incoming("sink-no-cs")
  public CompletionStage<Void> sinkNoForBuilder(Message<String> ignored) {
    return CompletableFuture.completedFuture(null);
  }

  @Incoming(MANUAL_ACKNOWLEDGMENT)
  @Outgoing("sink-manual")
  public Message<String> processorWithAck(Message<String> input) {
    sink.computeIfAbsent(MANUAL_ACKNOWLEDGMENT, x -> new CopyOnWriteArrayList<>()).add(input.getPayload());
    input.ack().toCompletableFuture().join();
    return input;
  }

  @Outgoing(MANUAL_ACKNOWLEDGMENT)
  public Publisher<Message<String>> sourceToManualAck() {
    return ReactiveStreams.of("a", "b", "c", "d", "e")
      .map(payload ->
        Message.of(payload, () -> CompletableFuture.runAsync(() -> {
          nap();
          acknowledged.computeIfAbsent(MANUAL_ACKNOWLEDGMENT, x -> new CopyOnWriteArrayList<>()).add(payload);
        }))
      ).buildRs();
  }

  @Incoming(NO_ACKNOWLEDGMENT)
  @Outgoing("sink-no")
  public Message<String> processorWithNoAck(Message<String> input) {
    sink.computeIfAbsent(NO_ACKNOWLEDGMENT, x -> new CopyOnWriteArrayList<>()).add(input.getPayload());
    return input;
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
  @Outgoing("sink-auto")
  public String processorWithAck(String input) {
    sink.computeIfAbsent(AUTO_ACKNOWLEDGMENT, x -> new CopyOnWriteArrayList<>()).add(input);
    return input;
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

  @Incoming(MANUAL_ACKNOWLEDGMENT_CS)
  @Outgoing("sink-manual-cs")
  public CompletionStage<Message<String>> processorWithAckWithBuilder(Message<String> message) {
    sink.computeIfAbsent(MANUAL_ACKNOWLEDGMENT_CS, x -> new CopyOnWriteArrayList<>()).add(message.getPayload());
    return message.ack().thenApply(x -> message);
  }

  @Outgoing(MANUAL_ACKNOWLEDGMENT_CS)
  public PublisherBuilder<Message<String>> sourceToManualAckWithBuilder() {
    return ReactiveStreams.of("a", "b", "c", "d", "e")
      .map(payload ->
        Message.of(payload, () -> CompletableFuture.runAsync(() -> {
          nap();
          acknowledged.computeIfAbsent(MANUAL_ACKNOWLEDGMENT_CS, x -> new CopyOnWriteArrayList<>()).add(payload);
        }))
      );
  }

  @Incoming(NO_ACKNOWLEDGMENT_CS)
  @Outgoing("sink-no-cs")
  public CompletionStage<Message<String>> processorWithNoAckWithBuilder(Message<String> message) {
    sink.computeIfAbsent(NO_ACKNOWLEDGMENT_CS, x -> new CopyOnWriteArrayList<>()).add(message.getPayload());
    return CompletableFuture.completedFuture(message);
  }

  @Outgoing(NO_ACKNOWLEDGMENT_CS)
  public Publisher<Message<String>> sourceToNoAckWithBuilder() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> {
        nap();
        acknowledged.computeIfAbsent(NO_ACKNOWLEDGMENT_CS, x -> new CopyOnWriteArrayList<>()).add(payload);
        return CompletableFuture.completedFuture(null);
      }));
  }

  @Incoming(AUTO_ACKNOWLEDGMENT_CS)
  @Outgoing("sink-auto-cs")
  public CompletionStage<String> processorWithAckWithBuilder(String payload) {
    sink.computeIfAbsent(AUTO_ACKNOWLEDGMENT_CS, x -> new CopyOnWriteArrayList<>()).add(payload);
    return CompletableFuture.completedFuture(payload);
  }

  @Outgoing(AUTO_ACKNOWLEDGMENT_CS)
  public Publisher<Message<String>> sourceToAutoAckWithBuilder() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> {
        nap();
        acknowledged.computeIfAbsent(AUTO_ACKNOWLEDGMENT_CS, x -> new CopyOnWriteArrayList<>()).add(payload);
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
