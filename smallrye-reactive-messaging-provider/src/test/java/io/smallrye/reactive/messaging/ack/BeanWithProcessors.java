package io.smallrye.reactive.messaging.ack;

import io.reactivex.Flowable;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@ApplicationScoped
public class BeanWithProcessors {

  static final String MANUAL_ACKNOWLEDGMENT = "manual-acknowledgment";
  static final String NO_ACKNOWLEDGMENT = "no-acknowledgment";
  static final String AUTO_ACKNOWLEDGMENT = "auto-acknowledgment";
  static final String MANUAL_ACKNOWLEDGMENT_BUILDER = "manual-acknowledgment-builder";
  static final String NO_ACKNOWLEDGMENT_BUILDER = "no-acknowledgment-builder";
  static final String AUTO_ACKNOWLEDGMENT_BUILDER = "auto-acknowledgment-builder";

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
  public void sinkManual(Message<String> ignored) {
    // do nothing
  }

  @Incoming("sink-auto")
  public void sinkAuto(Message<String> ignored) {
    // do nothing
  }

  @Incoming("sink-no")
  public void sinkNo(Message<String> ignored) {
    // do nothing
  }

  @Incoming("sink-manual-builder")
  public void sinkManualForBuilder(Message<String> ignored) {
    // do nothing
  }

  @Incoming("sink-auto-builder")
  public void sinkAutoForBuilder(Message<String> ignored) {
    // do nothing
  }

  @Incoming("sink-no-builder")
  public void sinkNoForBuilder(Message<String> ignored) {
    // do nothing
  }

  @Incoming(MANUAL_ACKNOWLEDGMENT)
  @Outgoing("sink-manual")
  public Processor<Message<String>, Message<String>> processorWithAck() {
    return ReactiveStreams.<Message<String>>builder()
      .flatMapCompletionStage(m -> m.ack().thenApply(x -> m))
      .peek(m -> sink.computeIfAbsent(MANUAL_ACKNOWLEDGMENT, x -> new CopyOnWriteArrayList<>()).add(m.getPayload()))
      .buildRs();
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
  public Processor<Message<String>, Message<String>> processorWithNoAck() {
    return ReactiveStreams.<Message<String>>builder()
      .peek(m -> sink.computeIfAbsent(NO_ACKNOWLEDGMENT, x -> new CopyOnWriteArrayList<>()).add(m.getPayload()))
      .buildRs();
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
  public Processor<String, String> processorWithAutoAck() {
    return ReactiveStreams.<String>builder()
      .peek(payload -> sink.computeIfAbsent(AUTO_ACKNOWLEDGMENT, x -> new CopyOnWriteArrayList<>()).add(payload))
      .buildRs();
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

  @Incoming(MANUAL_ACKNOWLEDGMENT_BUILDER)
  @Outgoing("sink-manual-builder")
  public ProcessorBuilder<Message<String>, Message<String>> processorWithAckWithBuilder() {
    return ReactiveStreams.<Message<String>>builder()
      .flatMapCompletionStage(m -> m.ack().thenApply(x -> m))
      .peek(m -> sink.computeIfAbsent(MANUAL_ACKNOWLEDGMENT_BUILDER, x -> new CopyOnWriteArrayList<>()).add(m.getPayload()));
  }

  @Outgoing(MANUAL_ACKNOWLEDGMENT_BUILDER)
  public PublisherBuilder<Message<String>> sourceToManualAckWithBuilder() {
    return ReactiveStreams.of("a", "b", "c", "d", "e")
      .map(payload ->
        Message.of(payload, () -> CompletableFuture.runAsync(() -> {
          nap();
          acknowledged.computeIfAbsent(MANUAL_ACKNOWLEDGMENT_BUILDER, x -> new CopyOnWriteArrayList<>()).add(payload);
        }))
      );
  }

  @Incoming(NO_ACKNOWLEDGMENT_BUILDER)
  @Outgoing("sink-no-builder")
  public ProcessorBuilder<Message<String>, Message<String>> processorWithNoAckWithBuilder() {
    return ReactiveStreams.<Message<String>>builder()
      .peek(m -> sink.computeIfAbsent(NO_ACKNOWLEDGMENT_BUILDER, x -> new CopyOnWriteArrayList<>()).add(m.getPayload()));
  }

  @Outgoing(NO_ACKNOWLEDGMENT_BUILDER)
  public Publisher<Message<String>> sourceToNoAckWithBuilder() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> {
        nap();
        acknowledged.computeIfAbsent(NO_ACKNOWLEDGMENT_BUILDER, x -> new CopyOnWriteArrayList<>()).add(payload);
        return CompletableFuture.completedFuture(null);
      }));
  }

  @Incoming(AUTO_ACKNOWLEDGMENT_BUILDER)
  @Outgoing("sink-auto-builder")
  public ProcessorBuilder<String, String> processorWithAutoAckBuilder() {
    return ReactiveStreams.<String>builder()
      .peek(payload -> sink.computeIfAbsent(AUTO_ACKNOWLEDGMENT_BUILDER, x -> new CopyOnWriteArrayList<>()).add(payload));
  }

  @Outgoing(AUTO_ACKNOWLEDGMENT_BUILDER)
  public Publisher<Message<String>> sourceToAutoAckWithBuilder() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> {
        nap();
        acknowledged.computeIfAbsent(AUTO_ACKNOWLEDGMENT_BUILDER, x -> new CopyOnWriteArrayList<>()).add(payload);
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
