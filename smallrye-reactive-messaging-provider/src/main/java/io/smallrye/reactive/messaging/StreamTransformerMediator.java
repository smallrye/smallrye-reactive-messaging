package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import org.apache.commons.lang3.ClassUtils;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import java.util.Objects;
import java.util.function.Function;

public class StreamTransformerMediator extends AbstractMediator {

  Function<Publisher<Message>, Publisher<Message>> function;

  private Publisher<Message> publisher;

  public StreamTransformerMediator(MediatorConfiguration configuration) {
    super(configuration);
  }

  @Override
  public void connectToUpstream(Publisher<? extends Message> publisher) {
    Objects.requireNonNull(function);
    this.publisher = decorate(function.apply((Publisher) publisher));
  }

  @Override
  public Publisher<Message> getComputedPublisher() {
    Objects.requireNonNull(publisher);
    return publisher;
  }

  @Override
  public boolean isConnected() {
    return publisher != null;
  }

  @Override
  public void initialize(Object bean) {
    // 1. Publisher<Message<O>> method(Publisher<Message<I>> publisher)
    // 2. Publisher<O> method(Publisher<I> publisher)
    // 3. PublisherBuilder<Message<O>> method(PublisherBuilder<Message<I>> publisher)
    // 4. PublisherBuilder<O> method(PublisherBuilder<I> publisher)

    switch (configuration.consumption()) {
      case STREAM_OF_MESSAGE:
        if (configuration.usesBuilderTypes()) {
          // Case 3
          processMethodConsumingAPublisherBuilderOfMessages(bean);
        } else {
          // Case 1
          processMethodConsumingAPublisherOfMessages(bean);
        }
        break;
      case STREAM_OF_PAYLOAD:
        if (configuration.usesBuilderTypes()) {
          // Case 4
          processMethodConsumingAPublisherBuilderOfPayload(bean);
        } else {
          // Case 2
          processMethodConsumingAPublisherOfPayload(bean);
        }
        break;
      default:
        throw new IllegalArgumentException("Unexpected consumption type: " + configuration.consumption());
    }

    assert function != null;
  }

  private void processMethodConsumingAPublisherBuilderOfMessages(Object bean) {
    function = publisher -> {
      PublisherBuilder<Message> prependedWithAck = ReactiveStreams.fromPublisher(publisher)
        .flatMapCompletionStage(managePreProcessingAck());

      PublisherBuilder<Message> builder = invoke(bean, prependedWithAck);
      Objects.requireNonNull(builder, "The method " + configuration.methodAsString() + " has returned an invalid value: `null`");
      return builder.buildRs();
    };
  }

  private void processMethodConsumingAPublisherOfMessages(Object bean) {
    function = publisher -> {
      Publisher<Message> prependedWithAck = ReactiveStreams.fromPublisher(publisher)
        .flatMapCompletionStage(managePreProcessingAck())
        .buildRs();
      Publisher<Message> result = invoke(bean, prependedWithAck);
      Objects.requireNonNull(result, "The method " + configuration.methodAsString() + " has returned an invalid value: `null`");
      return result;
    };
  }

  private void processMethodConsumingAPublisherBuilderOfPayload(Object bean) {
    function = publisher -> {
      PublisherBuilder<Object> stream = ReactiveStreams.fromPublisher(publisher)
        .flatMapCompletionStage(managePreProcessingAck())
        .map(Message::getPayload);
      PublisherBuilder<Object> builder = invoke(bean, stream);
      Objects.requireNonNull(builder, "The method " + configuration.methodAsString() + " has returned an invalid value: `null`");
      return builder
        .map(o -> (Message) Message.of(o))
        .buildRs();
    };
  }

  private void processMethodConsumingAPublisherOfPayload(Object bean) {
    function = publisher -> {
      Publisher stream = ReactiveStreams.fromPublisher(publisher)
        .flatMapCompletionStage(managePreProcessingAck())
        .map(Message::getPayload).buildRs();
      // Ability to inject a RX Flowable in method getting a Publisher.
      if (ClassUtils.isAssignable(configuration.getMethod().getParameterTypes()[0], Flowable.class)) {
        stream = Flowable.fromPublisher(publisher).map(Message::getPayload);
      }
      Publisher<Object> result = invoke(bean, stream);
      Objects.requireNonNull(result, "The method " + configuration.methodAsString() + " has returned an invalid value: `null`");
      return ReactiveStreams.fromPublisher(result)
        .map(o -> (Message) Message.of(o))
        .buildRs();
    };
  }

}
