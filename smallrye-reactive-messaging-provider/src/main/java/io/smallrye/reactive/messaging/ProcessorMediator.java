package io.smallrye.reactive.messaging;

import org.apache.commons.lang3.ClassUtils;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;


public class ProcessorMediator extends AbstractMediator {

  private Processor<Message, Message> processor;
  private Publisher<Message> publisher;

  public ProcessorMediator(MediatorConfiguration configuration) {
    super(configuration);
    if (configuration.shape() != Shape.PROCESSOR) {
      throw new IllegalArgumentException("Expected a Processor shape, received a " + configuration.shape());
    }
  }

  public void connectToUpstream(Publisher<? extends Message> publisher) {
    assert processor != null;
    this.publisher = ReactiveStreams.fromPublisher((Publisher) publisher).via(processor).buildRs();
  }

  @Override
  public Publisher<Message> getComputedPublisher() {
    return Objects.requireNonNull(publisher);
  }

  @Override
  public boolean isConnected() {
    return publisher != null;
  }

  @Override
  public void initialize(Object bean) {
    // Supported signatures:
    // 1.  Processor<Message<I>, Message<O>> method()
    // 2.  Processor<I, O> method()
    // 3.  ProcessorBuilder<Message<I>, Message<O>> method()
    // 4.  ProcessorBuilder<I, O> method()

    // 5.  Publisher<Message<O>> method(Message<I> msg)
    // 6.  Publisher<O> method(I payload)
    // 7.  PublisherBuilder<Message<O>> method(Message<I> msg)
    // 8.  PublisherBuilder<O> method(I payload)

    // 9. Message<O> method(Message<I> msg)
    // 10. O method(I payload)
    // 11. CompletionStage<O> method(I payload)
    // 12. CompletionStage<Message<O>> method(Message<I> msg)

    switch (configuration.production()) {
      case STREAM_OF_MESSAGE:
        // Case 1, 3, 5, 7
        if (isReturningAProcessorOrAProcessorBuilder()) {
          if (configuration.usesBuilderTypes()) {
            // Case 3
            processMethodReturningAProcessorBuilderOfMessages(bean);
          } else {
            // Case 1
            processMethodReturningAProcessorOfMessages(bean);
          }
        } else if (isReturningAPublisherOrAPublisherBuilder()) {
          if (configuration.usesBuilderTypes()) {
            // Case 7
            processMethodReturningAPublisherBuilderOfMessageAndConsumingMessages(bean);
          } else {
            // Case 5
            processMethodReturningAPublisherOfMessageAndConsumingMessages(bean);
          }
        } else {
          throw new IllegalArgumentException("Invalid Processor - unsupported signature for " + configuration.methodAsString());
        }
        break;
      case STREAM_OF_PAYLOAD:
        // Case 2, 4, 6, 8
        if (isReturningAProcessorOrAProcessorBuilder()) {
          // Case 2, 4
          if (configuration.usesBuilderTypes()) {
            // Case 4
            processMethodReturningAProcessorBuilderOfPayloads(bean);
          } else {
            // Case 2
            processMethodReturningAProcessorOfPayloads(bean);
          }
        } else if (isReturningAPublisherOrAPublisherBuilder()) {
          // Case 6, 8
          if (configuration.usesBuilderTypes()) {
            // Case 8
            processMethodReturningAPublisherBuilderOfPayloadsAndConsumingPayloads(bean);
          } else {
            // Case 6
            processMethodReturningAPublisherOfPayloadsAndConsumingPayloads(bean);
          }
        } else {
          throw new IllegalArgumentException("Invalid Processor - unsupported signature for " + configuration.methodAsString());
        }
        break;
      case INDIVIDUAL_MESSAGE:
        // Case 9
        processMethodReturningIndividualMessageAndConsumingIndividualItem(bean);
        break;
      case INDIVIDUAL_PAYLOAD:
        // Case 10
        processMethodReturningIndividualPayloadAndConsumingIndividualItem(bean);
        break;
      case COMPLETION_STAGE_OF_MESSAGE:
        // Case 11
        processMethodReturningACompletionStageOfMessageAndConsumingIndividualMessage(bean);
        break;
      case COMPLETION_STAGE_OF_PAYLOAD:
        // Case 12
        processMethodReturningACompletionStageOfPayloadndConsumingIndividualPayload(bean);
        break;
      default:
        throw new IllegalArgumentException("Unexpected production type: " + configuration.production());
    }
  }

  private void processMethodReturningAPublisherBuilderOfMessageAndConsumingMessages(Object bean) {
    this.processor = ReactiveStreams.<Message>builder()
      .map(msg -> (PublisherBuilder<Message>) invoke(bean, msg))
      .flatMap(Function.identity())
      .buildRs();
  }

  private void processMethodReturningAPublisherOfMessageAndConsumingMessages(Object bean) {
    this.processor = ReactiveStreams.<Message>builder()
      .map(msg -> (Publisher<Message>) invoke(bean, msg))
      .flatMapRsPublisher(Function.identity())
      .buildRs();
  }

  private void processMethodReturningAProcessorBuilderOfMessages(Object bean) {
    ProcessorBuilder<Message, Message> builder = Objects.requireNonNull(invoke(bean),
      "The method " + configuration.methodAsString() + " returned `null`");
    processor = builder.buildRs();
  }

  private void processMethodReturningAProcessorOfMessages(Object bean) {
    processor = Objects.requireNonNull(invoke(bean), "The method " + configuration.methodAsString() + " returned `null`");
  }

  private <I, O> void processMethodReturningAProcessorOfPayloads(Object bean) {
    Function<Message<I>, PublisherBuilder<? extends Message<O>>> function = msg -> {
      Processor<I, O> returnedProcessor = invoke(bean);
      Objects.requireNonNull(returnedProcessor, "The method " + configuration.methodAsString()
        + " has returned an invalid value: null");
      I input = msg.getPayload();
      return ReactiveStreams.of(input)
        .via(returnedProcessor)
        .map(Message::of)
        .flatMapCompletionStage(output -> getAckOrCompletion(msg).thenApply(x -> output));
    };

    ProcessorBuilder messageMessageProcessorBuilder = ReactiveStreams.<Message<I>>builder().flatMap(function);
    this.processor = messageMessageProcessorBuilder.buildRs();
  }

  private <I, O> void processMethodReturningAProcessorBuilderOfPayloads(Object bean) {
    ProcessorBuilder<I, O> returnedProcessorBuilder = invoke(bean);
    Objects.requireNonNull(returnedProcessorBuilder, "The method " + configuration.methodAsString()
      + " has returned an invalid value: null");
    Function<Message<I>, PublisherBuilder<? extends Message<O>>> function = msg -> {
      I input = msg.getPayload();
      return ReactiveStreams.of(input)
        .via(returnedProcessorBuilder)
        .map(Message::of)
        .flatMapCompletionStage(output -> getAckOrCompletion(msg).thenApply(x -> output));
    };

    ProcessorBuilder messageMessageProcessorBuilder = ReactiveStreams.<Message<I>>builder().flatMap(function);
    this.processor = messageMessageProcessorBuilder.buildRs();
  }

  private void processMethodReturningAPublisherBuilderOfPayloadsAndConsumingPayloads(Object bean) {
    Function<Message, PublisherBuilder<? extends Message>> function = message -> {
      PublisherBuilder builder = invoke(bean, message.getPayload());
      return ReactiveStreams.of(message)
        .flatMapCompletionStage(m -> getAckOrCompletion(m).thenApply(x -> m))
        .flatMap(v -> builder)
        .map(Message::of);
    };

    this.processor = ReactiveStreams.<Message>builder().flatMap(function).buildRs();
  }

  private void processMethodReturningAPublisherOfPayloadsAndConsumingPayloads(Object bean) {
    Function<Message, PublisherBuilder<? extends Message>> function = message -> {
      Publisher publisher = invoke(bean, message.getPayload());
      return ReactiveStreams.of(message)
        .flatMapCompletionStage(m -> getAckOrCompletion(m)
          .thenApply(x -> m))
        .flatMapRsPublisher(v -> publisher)
        .map(Message::of);
    };

    this.processor = ReactiveStreams.<Message>builder()
      .flatMap(function).buildRs();
  }

  private void processMethodReturningIndividualMessageAndConsumingIndividualItem(Object bean) {
    // Item can be message or payload
    if (configuration.consumption() == MediatorConfiguration.Consumption.PAYLOAD) {
      this.processor = ReactiveStreams.<Message>builder().map(input -> (Message) invoke(bean, input.getPayload())).buildRs();
    } else {
      this.processor = ReactiveStreams.<Message>builder().map(input -> (Message) invoke(bean, input)).buildRs();
    }
  }

  private void processMethodReturningIndividualPayloadAndConsumingIndividualItem(Object bean) {
    // Item can be message or payload.
    if (configuration.consumption() == MediatorConfiguration.Consumption.PAYLOAD) {
      this.processor = ReactiveStreams.<Message>builder()
        .flatMapCompletionStage(input -> {
          Object result = invoke(bean, input.getPayload());
          return getAckOrCompletion(input).thenApply(x -> (Message) Message.of(result));
        })
        .buildRs();
    } else {
      this.processor = ReactiveStreams.<Message>builder()
        .flatMapCompletionStage(input -> {
          Object result = invoke(bean, input);
          return getAckOrCompletion(input).thenApply(x -> (Message) Message.of(result));
        })
        .buildRs();
    }
  }

  private void processMethodReturningACompletionStageOfMessageAndConsumingIndividualMessage(Object bean) {
    this.processor = ReactiveStreams.<Message>builder()
      .<Message>flatMapCompletionStage(input -> invoke(bean, input))
      .buildRs();
  }

  private void processMethodReturningACompletionStageOfPayloadndConsumingIndividualPayload(Object bean) {
    this.processor = ReactiveStreams.<Message>builder()
      .flatMapCompletionStage(input -> {
        CompletionStage<Object> cs = invoke(bean, input.getPayload());
        return cs.thenCompose(p -> getAckOrCompletion(input).thenApply(x -> (Message) Message.of(p)));
      })
      .buildRs();
  }


  private boolean isReturningAPublisherOrAPublisherBuilder() {
    Class<?> returnType = configuration.getMethod().getReturnType();
    return ClassUtils.isAssignable(returnType, Publisher.class) || ClassUtils.isAssignable(returnType, PublisherBuilder.class);
  }

  private boolean isReturningAProcessorOrAProcessorBuilder() {
    Class<?> returnType = configuration.getMethod().getReturnType();
    return ClassUtils.isAssignable(returnType, Processor.class) || ClassUtils.isAssignable(returnType, ProcessorBuilder.class);
  }
}
