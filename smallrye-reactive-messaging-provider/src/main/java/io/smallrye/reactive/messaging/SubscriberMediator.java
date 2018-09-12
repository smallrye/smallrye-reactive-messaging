package io.smallrye.reactive.messaging;

import org.apache.commons.lang3.ClassUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

public class SubscriberMediator extends AbstractMediator {

  private Publisher<Message> source;
  private Subscriber subscriber;
  /**
   * Keep track of the subscription to cancel it once the scope is terminated.
   */
  private AtomicReference<Subscription> subscription = new AtomicReference<>();


  // Supported signatures:
  // 1. Subscriber<Message<I>> method()
  // 2. Subscriber<I> method()
  // 3. CompletionStage<?> method(Message<I> m)
  // 4. CompletionStage<?> method(I i)
  // 5. void/? method(Message<I> m) - The support of this method has been removed (CES - Reactive Hangout 2018/09/11).
  // 6. void/? method(I i)

  public SubscriberMediator(MediatorConfiguration configuration) {
    super(configuration);
    if (configuration.shape() != Shape.SUBSCRIBER) {
      throw new IllegalArgumentException("Expected a Subscriber shape, received a " + configuration.shape());
    }
  }

  @Override
  public void initialize(Object bean) {
    switch (configuration.consumption()) {
      case STREAM_OF_MESSAGE: // 1
      case STREAM_OF_PAYLOAD: // 2
        processMethodReturningASubscriber(bean);
        break;
      case MESSAGE: // 3  (5 being dropped)
      case PAYLOAD: // 4 or 6
        if (ClassUtils.isAssignable(configuration.getMethod().getReturnType(), CompletionStage.class)) {
          // Case 3, 4
          processMethodReturningACompletionStage(bean);
        } else {
          // Case 6 (5 being dropped)
          processMethodReturningVoid(bean);
        }
        break;
      default:
        throw new IllegalArgumentException("Unexpected consumption type: " + configuration.consumption());
    }

    assert this.subscriber != null;
  }

  @Override
  public Subscriber<Message> getComputedSubscriber() {
    return subscriber;
  }

  @Override
  public boolean isConnected() {
    return source != null;
  }

  @Override
  public void connectToUpstream(Publisher<? extends Message> publisher) {
    this.source = (Publisher) publisher;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void run() {
    assert this.source != null;
    assert this.subscriber != null;
    final Logger logger = LogManager.getLogger(configuration.methodAsString());
    Subscriber delegating = new Subscriber() {
      @Override
      public void onSubscribe(Subscription s) {
        subscription.set(s);
        subscriber.onSubscribe(s);
      }

      @Override
      public void onNext(Object o) {
        subscriber.onNext(o);
      }

      @Override
      public void onError(Throwable t) {
        logger.error("Error caught during the stream processing", t);
        subscriber.onError(t);
      }

      @Override
      public void onComplete() {
        subscriber.onComplete();
      }
    };

    this.source.subscribe(delegating);
  }

  private void processMethodReturningVoid(Object bean) {
    if (configuration.consumption() == MediatorConfiguration.Consumption.PAYLOAD) {
      this.subscriber = ReactiveStreams.<Message<?>>builder()
        .flatMapCompletionStage(message -> {
          invoke(bean, message.getPayload());
          return getAckOrCompletion(message).thenApply(x -> message);
        })
        .ignore()
        .build();
    }
  }

  private void processMethodReturningACompletionStage(Object bean) {
    if (configuration.consumption() == MediatorConfiguration.Consumption.PAYLOAD) {
      this.subscriber = ReactiveStreams.<Message<?>>builder()
        .flatMapCompletionStage(message -> {
          CompletionStage<?> stage = invoke(bean, message.getPayload());
          return stage.thenApply(x -> getAckOrCompletion(message)).thenApply(x -> message);
        })
        .ignore()
        .build();
    } else {
      this.subscriber = ReactiveStreams.<Message<?>>builder()
        .flatMapCompletionStage(message -> {
          CompletionStage<?> completion = invoke(bean, message);
          return completion.thenApply(x -> message);
        })
        .ignore()
        .build();
    }
  }

  private void processMethodReturningASubscriber(Object bean) {
    Object result = invoke(bean);
    if (!(result instanceof Subscriber)) {
      throw new IllegalStateException("Invalid return type: " + result + " - expected a Subscriber");
    }
    Subscriber sub  = (Subscriber) result;
    if (configuration.consumption() == MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD) {
      // TODO Is it the expected behavior, the ack happen before the subscriber to be called.
      this.subscriber = ReactiveStreams.<Message>builder()
        .flatMapCompletionStage(message -> getAckOrCompletion(message).thenApply(x -> message))
        .map(Message::getPayload)
        .to(sub)
        .build();
    } else {
      this.subscriber = sub ;
    }
  }
}
