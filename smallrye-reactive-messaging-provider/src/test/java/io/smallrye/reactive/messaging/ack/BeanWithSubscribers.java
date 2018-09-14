package io.smallrye.reactive.messaging.ack;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletableFuture;

@ApplicationScoped
public class BeanWithSubscribers extends SpiedBeanHelper {

  public static final String MANUAL_ACKNOWLEDGMENT = "manual-acknowledgment";
  public static final String NO_ACKNOWLEDGMENT = "no-acknowledgment";
  public static final String PRE_PROCESSING_ACK = "pre-processing-acknowledgment";
  public static final String POST_PROCESSING_ACK = "post-processing-acknowledgment";
  public static final String DEFAULT_PROCESSING_ACK = "default-processing-acknowledgment";

  @Incoming(MANUAL_ACKNOWLEDGMENT)
  @Acknowledgment(Acknowledgment.Mode.MANUAL)
  public Subscriber<Message<String>> subWithAck() {
    return ReactiveStreams.<Message<String>>builder()
      .flatMapCompletionStage(m -> m.ack().thenApply(x -> m))
      .forEach(m -> {
        processed(MANUAL_ACKNOWLEDGMENT, m.getPayload());
        microNap();
      })
      .build();
  }

  @Outgoing(MANUAL_ACKNOWLEDGMENT)
  public Publisher<Message<String>> sourceToManualAck() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> CompletableFuture.runAsync(() -> {
          nap();
          acknowledged(MANUAL_ACKNOWLEDGMENT, payload);
        }))
      );
  }

  @Incoming(NO_ACKNOWLEDGMENT)
  @Acknowledgment(Acknowledgment.Mode.NONE)
  public Subscriber<Message<String>> subWithNoAck() {
    return ReactiveStreams.<Message<String>>builder()
      .forEach(m -> {
        processed(NO_ACKNOWLEDGMENT, m.getPayload());
        microNap();
      })
      .build();
  }

  @Outgoing(NO_ACKNOWLEDGMENT)
  public Publisher<Message<String>> sourceToNoAck() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> {
        nap();
        acknowledged(NO_ACKNOWLEDGMENT, payload);
        return CompletableFuture.completedFuture(null);
      }));
  }

  //TODO Implement pre and post processing.
  // TODO How to check that it happened before or after
  // TODO Test auto.


  @Incoming(PRE_PROCESSING_ACK)
  @Acknowledgment(Acknowledgment.Mode.PRE_PROCESSING)
  public Subscriber<String> subWithPreAck() {
    return ReactiveStreams.<String>builder()
      .forEach(m -> {
        microNap();
        processed(PRE_PROCESSING_ACK, m);
      })
      .build();
  }

  @Outgoing(PRE_PROCESSING_ACK)
  public Publisher<Message<String>> sourceToPreAck() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> {
        acknowledged(PRE_PROCESSING_ACK, payload);
        nap();
        return CompletableFuture.completedFuture(null);
      }));
  }

  @Incoming(POST_PROCESSING_ACK)
  @Acknowledgment(Acknowledgment.Mode.POST_PROCESSING)
  public Subscriber<String> subWithPostAck() {
    return ReactiveStreams.<String>builder()
      .forEach(m -> {
        processed(POST_PROCESSING_ACK, m);
        microNap();
      })
      .build();
  }

  @Outgoing(POST_PROCESSING_ACK)
  public Publisher<Message<String>> sourceToPostAck() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> {
        acknowledged(POST_PROCESSING_ACK, payload);
        nap();
        return CompletableFuture.completedFuture(null);
      }));
  }

  @Incoming(DEFAULT_PROCESSING_ACK)
  public Subscriber<String> subWithDefaultAck() {
    return ReactiveStreams.<String>builder()
      .forEach(m -> {
        processed(DEFAULT_PROCESSING_ACK, m);
        microNap();
      })
      .build();
  }

  @Outgoing(DEFAULT_PROCESSING_ACK)
  public Publisher<Message<String>> sourceToDefaultAck() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> {
        nap();
        acknowledged(DEFAULT_PROCESSING_ACK, payload);
        return CompletableFuture.completedFuture(null);
      }));
  }





}
