package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import org.apache.commons.lang3.ClassUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class SubscriberMediator extends AbstractMediator {

  private Publisher<Message> source;
  private Subscriber subscriber;


  // Supported signatures:
  // 1. Subscriber<Message<I>> method()
  // 2. Subscriber<I> method()
  // 3. CompletionStage<?> method(Message<I> m)
  // 4. CompletionStage<?> method(I i)
  // 5. void/? method(Message<I> m)
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
      case MESSAGE: // 3 or 5
      case PAYLOAD: // 4 or 6
        if (ClassUtils.isAssignable(configuration.getMethod().getReturnType(), CompletionStage.class)) {
          // Case 3, 4
          processMethodReturningACompletionStage(bean);
        } else {
          // Case 5 or 6
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
        System.out.println("Subscription attempt using " + s);
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

    Flowable.fromPublisher(this.source)
      .doOnSubscribe(x -> System.out.println("In on subscribe"))
      .doOnRequest(x -> System.out.println("Got request"))
      .subscribe(delegating);
  }

  private void processMethodReturningVoid(Object bean) {
    if (configuration.consumption() == MediatorConfiguration.Consumption.PAYLOAD) {
      this.subscriber = ReactiveStreams.<Message<?>>builder()
        .flatMapCompletionStage(message -> {
          invoke(bean, message.getPayload());
          return getAckOrCompletion(message);
        })
        .ignore()
        .build();
    } else {
      this.subscriber = ReactiveStreams.<Message<?>>builder()
        .flatMapCompletionStage(message -> {
          CompletionStage<?> stage = invoke(bean, message);
          return stage
            .thenCompose(x -> getAckOrCompletion(message));
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
      // TODO manage acknowledgment
      this.subscriber = ReactiveStreams.<Message>builder()
        .map(Message::getPayload)
        .to(sub)
        .build();
    } else {
      this.subscriber = sub ;
    }
  }
}
