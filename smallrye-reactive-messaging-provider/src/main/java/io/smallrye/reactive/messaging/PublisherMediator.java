package io.smallrye.reactive.messaging;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class PublisherMediator extends AbstractMediator {

  private Publisher publisher;

  // Supported signatures:
  // 1. Publisher<Message<O>> method()
  // 2. Publisher<O> method()
  // 3. PublisherBuilder<Message<O>> method()
  // 4. PublisherBuilder<O> method()
  // 5. O method() O cannot be Void
  // 6. Message<O> method()
  // 7. CompletionStage<O> method()
  // 8. CompletionStage<Message<O>> method()

  public PublisherMediator(MediatorConfiguration configuration) {
    super(configuration);
    if (configuration.shape() != Shape.PUBLISHER) {
      throw new IllegalArgumentException("Expected a Publisher shape, received a " + configuration.shape());
    }
  }

  @Override
  public Publisher<Message> getComputedPublisher() {
    return Objects.requireNonNull(publisher);
  }

  @Override
  public boolean isConnected() {
    // Easy, not expecting anything
    return true;
  }

  @Override
  public void initialize(Object bean) {
    switch (configuration.production()) {
      case STREAM_OF_MESSAGE: // 1, 3
        if (configuration.usesBuilderTypes()) {
          produceAPublisherBuilderOfMessages(bean);
        } else {
          produceAPublisherOfMessages(bean);
        }
        break;
      case STREAM_OF_PAYLOAD: // 2, 4
        if (configuration.usesBuilderTypes()) {
          produceAPublisherBuilderOfPayloads(bean);
        } else {
          produceAPublisherOfPayloads(bean);
        }
        break;
      case INDIVIDUAL_PAYLOAD: // 5
        produceIndividualPayloads(bean);
        break;
      case INDIVIDUAL_MESSAGE:  // 6
        produceIndividualMessages(bean);
        break;
      case COMPLETION_STAGE_OF_MESSAGE: // 8
        produceIndividualCompletionStageOfMessages(bean);
        break;
      case COMPLETION_STAGE_OF_PAYLOAD: // 7
        produceIndividualCompletionStageOfPayloads(bean);
        break;
      default: throw new IllegalArgumentException("Unexpected production type: " + configuration.production());
    }

    assert this.publisher != null;
  }

  private void produceAPublisherBuilderOfMessages(Object bean) {
    PublisherBuilder<Message> builder = invoke(bean);
    setPublisher(builder.buildRs());
  }

  private void setPublisher(Publisher publisher) {
    this.publisher = decorate(publisher);
  }

  private <P> void produceAPublisherBuilderOfPayloads(Object bean) {
    PublisherBuilder<P> builder = invoke(bean);
    setPublisher(builder.map(Message::of).buildRs());
  }

  private void produceAPublisherOfMessages(Object bean) {
    setPublisher(invoke(bean));
  }

  private <P> void produceAPublisherOfPayloads(Object bean) {
    Publisher<P> pub = invoke(bean);
    setPublisher(ReactiveStreams.fromPublisher(pub)
      .map(Message::of).buildRs());
  }

  private void produceIndividualMessages(Object bean) {
    setPublisher(ReactiveStreams.generate(() -> {
      Message message = invoke(bean);
      Objects.requireNonNull(message, "The method " + configuration.methodAsString() + " returned an invalid value: null");
      return message;
    }).buildRs());
  }

  private <T> void produceIndividualPayloads(Object bean) {
    setPublisher(ReactiveStreams.<T>generate(() -> invoke(bean))
      .map(Message::of)
      .buildRs());
  }

  private void produceIndividualCompletionStageOfMessages(Object bean) {
    setPublisher(ReactiveStreams.<CompletionStage<Message>>generate(() -> invoke(bean))
      .flatMapCompletionStage(Function.identity())
      .buildRs());
  }

  private <P> void produceIndividualCompletionStageOfPayloads(Object bean) {
    setPublisher(ReactiveStreams.<CompletionStage<P>>generate(() -> invoke(bean))
      .flatMapCompletionStage(Function.identity())
      .map(Message::of)
      .buildRs());
  }
}
