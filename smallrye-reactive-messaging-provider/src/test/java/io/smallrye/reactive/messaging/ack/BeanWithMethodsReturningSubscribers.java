package io.smallrye.reactive.messaging.ack;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@ApplicationScoped
public class BeanWithMethodsReturningSubscribers extends SpiedBeanHelper {

  public static final String MANUAL_ACKNOWLEDGMENT_MESSAGE = "manual-acknowledgment-message";

  public static final String NO_ACKNOWLEDGMENT_MESSAGE = "no-acknowledgment-message";
  public static final String NO_ACKNOWLEDGMENT_PAYLOAD = "no-acknowledgment-payload";

  public static final String PRE_PROCESSING_ACK_MESSAGE = "pre-processing-acknowledgment-message";
  public static final String PRE_PROCESSING_ACK_PAYLOAD = "pre-processing-acknowledgment-payload";

  public static final String POST_PROCESSING_ACK_MESSAGE = "post-processing-acknowledgment-message";
  public static final String POST_PROCESSING_ACK_PAYLOAD = "post-processing-acknowledgment-payload";

  public static final String DEFAULT_PROCESSING_ACK_MESSAGE = "default-processing-acknowledgment-message";
  public static final String DEFAULT_PROCESSING_ACK_PAYLOAD = "default-processing-acknowledgment-payload";

  @Incoming(MANUAL_ACKNOWLEDGMENT_MESSAGE)
  @Acknowledgment(Acknowledgment.Mode.MANUAL)
  public Subscriber<Message<String>> subWithAckWithMessage() {
    return ReactiveStreams.<Message<String>>builder()
      .flatMapCompletionStage(m -> m.ack().thenApply(x -> m))
      .forEach(m -> {
        processed(MANUAL_ACKNOWLEDGMENT_MESSAGE, m.getPayload());
        microNap();
      })
      .build();
  }

  @Outgoing(MANUAL_ACKNOWLEDGMENT_MESSAGE)
  public Publisher<Message<String>> sourceToManualAckWithMessage() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> CompletableFuture.runAsync(() -> {
        nap();
        acknowledged(MANUAL_ACKNOWLEDGMENT_MESSAGE, payload);
        }))
      );
  }


  @Incoming(NO_ACKNOWLEDGMENT_MESSAGE)
  @Acknowledgment(Acknowledgment.Mode.NONE)
  public Subscriber<Message<String>> subWithNoAckWithMessage() {
    return ReactiveStreams.<Message<String>>builder()
      .forEach(m -> {
        processed(NO_ACKNOWLEDGMENT_MESSAGE, m.getPayload());
        microNap();
      })
      .build();
  }

  @Outgoing(NO_ACKNOWLEDGMENT_MESSAGE)
  public Publisher<Message<String>> sourceToNoAckWithMessage() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> {
        nap();
        acknowledged(NO_ACKNOWLEDGMENT_MESSAGE, payload);
        return CompletableFuture.completedFuture(null);
      }));
  }


  @Incoming(NO_ACKNOWLEDGMENT_PAYLOAD)
  @Acknowledgment(Acknowledgment.Mode.NONE)
  public Subscriber<String> subWithNoAckWithPayload() {
    return ReactiveStreams.<String>builder()
      .forEach(m -> {
        processed(NO_ACKNOWLEDGMENT_PAYLOAD, m);
        microNap();
      })
      .build();
  }

  @Outgoing(NO_ACKNOWLEDGMENT_PAYLOAD)
  public Publisher<Message<String>> sourceToNoAckWithPayload() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> {
        nap();
        acknowledged(NO_ACKNOWLEDGMENT_PAYLOAD, payload);
        return CompletableFuture.completedFuture(null);
      }));
  }

  @Incoming(PRE_PROCESSING_ACK_MESSAGE)
  @Acknowledgment(Acknowledgment.Mode.PRE_PROCESSING)
  public Subscriber<String> subWithPreAckWithMessage() {
    return ReactiveStreams.<String>builder()
      .forEach(m -> {
        microNap();
        processed(PRE_PROCESSING_ACK_MESSAGE, m);
      })
      .build();
  }

  @Outgoing(PRE_PROCESSING_ACK_MESSAGE)
  public Publisher<Message<String>> sourceToPreAckWithMessage() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> {
        acknowledged(PRE_PROCESSING_ACK_MESSAGE, payload);
        nap();
        return CompletableFuture.completedFuture(null);
      }));
  }



  @Incoming(PRE_PROCESSING_ACK_PAYLOAD)
  @Acknowledgment(Acknowledgment.Mode.PRE_PROCESSING)
  public Subscriber<String> subWithPreAckWithPayload() {
    return ReactiveStreams.<String>builder()
      .forEach(m -> {
        microNap();
        processed(PRE_PROCESSING_ACK_PAYLOAD, m);
      })
      .build();
  }

  @Outgoing(PRE_PROCESSING_ACK_PAYLOAD)
  public Publisher<Message<String>> sourceToPreAckWithPayload() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> {
        acknowledged(PRE_PROCESSING_ACK_PAYLOAD, payload);
        nap();
        return CompletableFuture.completedFuture(null);
      }));
  }

  @Incoming(POST_PROCESSING_ACK_MESSAGE)
  @Acknowledgment(Acknowledgment.Mode.POST_PROCESSING)
  public Subscriber<Message<String>> subWithPostAckWithMessage() {
    return ReactiveStreams.<Message<String>>builder()
      .forEach(m -> {
        processed(POST_PROCESSING_ACK_MESSAGE, m.getPayload());
        microNap();
      })
      .build();
  }

  @Outgoing(POST_PROCESSING_ACK_MESSAGE)
  public Publisher<Message<String>> sourceToPostAckWithMessage() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> {
        acknowledged(POST_PROCESSING_ACK_MESSAGE, payload);
        nap();
        return CompletableFuture.completedFuture(null);
      }));
  }

  @Incoming(POST_PROCESSING_ACK_PAYLOAD)
  @Acknowledgment(Acknowledgment.Mode.POST_PROCESSING)
  public Subscriber<String> subWithPostAckWithPayload() {
    return ReactiveStreams.<String>builder()
      .forEach(m -> {
        processed(POST_PROCESSING_ACK_PAYLOAD, m);
        microNap();
      })
      .build();
  }

  @Outgoing(POST_PROCESSING_ACK_PAYLOAD)
  public Publisher<Message<String>> sourceToPostAckWithPayload() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> {
        acknowledged(POST_PROCESSING_ACK_PAYLOAD, payload);
        nap();
        return CompletableFuture.completedFuture(null);
      }));
  }

  @Incoming(DEFAULT_PROCESSING_ACK_PAYLOAD)
  public Subscriber<String> subWithDefaultAckWithPayload() {
    return ReactiveStreams.<String>builder()
      .forEach(m -> {
        processed(DEFAULT_PROCESSING_ACK_PAYLOAD, m);
        microNap();
      })
      .build();
  }

  @Outgoing(DEFAULT_PROCESSING_ACK_PAYLOAD)
  public Publisher<Message<String>> sourceToDefaultAckWithPayload() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> {
        nap();
        acknowledged(DEFAULT_PROCESSING_ACK_PAYLOAD, payload);
        return CompletableFuture.completedFuture(null);
      }));
  }

  @Incoming(DEFAULT_PROCESSING_ACK_MESSAGE)
  public Subscriber<Message<String>> subWithDefaultAckWithMessage() {
    return ReactiveStreams.<Message<String>>builder()
      .forEach(m -> {
        processed(DEFAULT_PROCESSING_ACK_MESSAGE, m.getPayload());
        microNap();
      })
      .build();
  }

  @Outgoing(DEFAULT_PROCESSING_ACK_MESSAGE)
  public Publisher<Message<String>> sourceToDefaultAckWithMessage() {
    return Flowable.fromArray("a", "b", "c", "d", "e")
      .map(payload -> Message.of(payload, () -> {
        nap();
        acknowledged(DEFAULT_PROCESSING_ACK_MESSAGE, payload);
        return CompletableFuture.completedFuture(null);
      }));
  }


}
