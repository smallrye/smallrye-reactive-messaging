package io.smallrye.reactive.messaging;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.helpers.ClassUtils;

public class ProcessorMediator extends AbstractMediator {

    private Processor<Message, Message> processor;
    private PublisherBuilder<? extends Message> publisher;

    public ProcessorMediator(MediatorConfiguration configuration) {
        super(configuration);
        if (configuration.shape() != Shape.PROCESSOR) {
            throw new IllegalArgumentException("Expected a Processor shape, received a " + configuration.shape());
        }
    }

    @Override
    public void connectToUpstream(PublisherBuilder<? extends Message> publisher) {
        assert processor != null;
        this.publisher = decorate(publisher.via(processor));
    }

    @Override
    public PublisherBuilder<? extends Message> getStream() {
        return Objects.requireNonNull(publisher);
    }

    @Override
    public boolean isConnected() {
        return publisher != null;
    }

    @Override
    public void initialize(Object bean) {
        super.initialize(bean);
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
                        processMethodReturningAProcessorBuilderOfMessages();
                    } else {
                        // Case 1
                        processMethodReturningAProcessorOfMessages();
                    }
                } else if (isReturningAPublisherOrAPublisherBuilder()) {
                    if (configuration.usesBuilderTypes()) {
                        // Case 7
                        processMethodReturningAPublisherBuilderOfMessageAndConsumingMessages();
                    } else {
                        // Case 5
                        processMethodReturningAPublisherOfMessageAndConsumingMessages();
                    }
                } else {
                    throw new IllegalArgumentException(
                            "Invalid Processor - unsupported signature for " + configuration.methodAsString());
                }
                break;
            case STREAM_OF_PAYLOAD:
                // Case 2, 4, 6, 8
                if (isReturningAProcessorOrAProcessorBuilder()) {
                    // Case 2, 4
                    if (configuration.usesBuilderTypes()) {
                        // Case 4
                        processMethodReturningAProcessorBuilderOfPayloads();
                    } else {
                        // Case 2
                        processMethodReturningAProcessorOfPayloads();
                    }
                } else if (isReturningAPublisherOrAPublisherBuilder()) {
                    // Case 6, 8
                    if (configuration.usesBuilderTypes()) {
                        // Case 8
                        processMethodReturningAPublisherBuilderOfPayloadsAndConsumingPayloads();
                    } else {
                        // Case 6
                        processMethodReturningAPublisherOfPayloadsAndConsumingPayloads();
                    }
                } else {
                    throw new IllegalArgumentException(
                            "Invalid Processor - unsupported signature for " + configuration.methodAsString());
                }
                break;
            case INDIVIDUAL_MESSAGE:
                // Case 9
                processMethodReturningIndividualMessageAndConsumingIndividualItem();
                break;
            case INDIVIDUAL_PAYLOAD:
                // Case 10
                processMethodReturningIndividualPayloadAndConsumingIndividualItem();
                break;
            case COMPLETION_STAGE_OF_MESSAGE:
                // Case 11
                processMethodReturningACompletionStageOfMessageAndConsumingIndividualMessage();
                break;
            case COMPLETION_STAGE_OF_PAYLOAD:
                // Case 12
                processMethodReturningACompletionStageOfPayloadAndConsumingIndividualPayload();
                break;
            case UNI_OF_MESSAGE:
                // Case 11 - Uni variant
                processMethodReturningAUniOfMessageAndConsumingIndividualMessage();
                break;
            case UNI_OF_PAYLOAD:
                // Case 12 - Uni variant
                processMethodReturningAUniOfPayloadAndConsumingIndividualPayload();
                break;
            default:
                throw new IllegalArgumentException("Unexpected production type: " + configuration.production());
        }
    }

    private void processMethodReturningAPublisherBuilderOfMessageAndConsumingMessages() {
        this.processor = ReactiveStreams.<Message> builder()
                .flatMapCompletionStage(managePreProcessingAck())
                .map(msg -> (PublisherBuilder<Message>) invoke(msg))
                .flatMap(Function.identity())
                .buildRs();
    }

    private void processMethodReturningAPublisherOfMessageAndConsumingMessages() {
        this.processor = ReactiveStreams.<Message> builder()
                .flatMapCompletionStage(managePreProcessingAck())
                .map(msg -> (Publisher<Message>) invoke(msg))
                .flatMapRsPublisher(Function.identity())
                .buildRs();
    }

    private void processMethodReturningAProcessorBuilderOfMessages() {
        ProcessorBuilder<Message, Message> builder = Objects.requireNonNull(invoke(),
                "The method " + configuration.methodAsString() + " returned `null`");

        this.processor = ReactiveStreams.<Message> builder()
                .flatMapCompletionStage(managePreProcessingAck())
                .via(builder)
                .buildRs();
    }

    private void processMethodReturningAProcessorOfMessages() {
        Processor<Message, Message> result = Objects.requireNonNull(invoke(),
                "The method " + configuration.methodAsString() + " returned `null`");
        this.processor = ReactiveStreams.<Message> builder()
                .<Message> flatMapCompletionStage(msg -> {
                    if (configuration.getAcknowledgment() == Acknowledgment.Strategy.PRE_PROCESSING) {
                        return msg.ack().thenApply((x -> msg));
                    } else {
                        return CompletableFuture.completedFuture(msg);
                    }
                })
                .via(result)
                .buildRs();
    }

    private void processMethodReturningAProcessorOfPayloads() {
        Processor returnedProcessor = invoke();

        this.processor = ReactiveStreams.<Message> builder()
                .flatMapCompletionStage(managePreProcessingAck())
                .map(m -> m.getPayload())
                .via(returnedProcessor)
                .map(Message::of)
                .buildRs();
    }

    private void processMethodReturningAProcessorBuilderOfPayloads() {
        ProcessorBuilder returnedProcessorBuilder = invoke();
        Objects.requireNonNull(returnedProcessorBuilder, "The method " + configuration.methodAsString()
                + " has returned an invalid value: null");

        this.processor = ReactiveStreams.<Message> builder()
                .flatMapCompletionStage(managePreProcessingAck())
                .map(m -> m.getPayload())
                .via(returnedProcessorBuilder)
                .map(Message::of)
                .buildRs();
    }

    @SuppressWarnings("unchecked")
    private void processMethodReturningAPublisherBuilderOfPayloadsAndConsumingPayloads() {
        this.processor = ReactiveStreams.<Message> builder()
                .flatMapCompletionStage(managePreProcessingAck())
                .flatMap(message -> {
                    PublisherBuilder pb = invoke(message.getPayload());
                    return pb.map(payload -> Message.of(payload, message.getMetadata()));
                    // TODO We can handle post-acknowledgement here.
                })
                .buildRs();
    }

    @SuppressWarnings("unchecked")
    private void processMethodReturningAPublisherOfPayloadsAndConsumingPayloads() {
        this.processor = ReactiveStreams.<Message> builder()
                .flatMapCompletionStage(managePreProcessingAck())
                .flatMap(message -> {
                    Publisher pub = invoke(message.getPayload());
                    return ReactiveStreams.fromPublisher(pub)
                            .map(payload -> Message.of(payload, message.getMetadata()));
                    // TODO We can handle post-acknowledgement here.
                })
                .buildRs();
    }

    private void processMethodReturningIndividualMessageAndConsumingIndividualItem() {
        // Item can be message or payload
        if (configuration.consumption() == MediatorConfiguration.Consumption.PAYLOAD) {
            this.processor = ReactiveStreams.<Message> builder()
                    .flatMapCompletionStage(managePreProcessingAck())
                    .map(input -> (Message) invoke(input.getPayload()))
                    .buildRs();
        } else {
            this.processor = ReactiveStreams.<Message> builder()
                    .flatMapCompletionStage(managePreProcessingAck())
                    .map(input -> (Message) invoke(input))
                    .buildRs();
        }
    }

    private void processMethodReturningIndividualPayloadAndConsumingIndividualItem() {
        // Item can be message or payload.
        if (configuration.consumption() == MediatorConfiguration.Consumption.PAYLOAD) {
            this.processor = ReactiveStreams.<Message> builder()
                    .flatMapCompletionStage(managePreProcessingAck())
                    .<Message> map(input -> {
                        Object result = invoke(input.getPayload());
                        if (configuration.getAcknowledgment() == Acknowledgment.Strategy.POST_PROCESSING) {
                            return input.withPayload(result);
                        } else {
                            return Message.of(result, input.getMetadata());
                        }
                    })
                    .buildRs();
        } else {
            this.processor = ReactiveStreams.<Message> builder()
                    .flatMapCompletionStage(managePreProcessingAck())
                    .<Message> map(input -> {
                        Object result = invoke(input);
                        if (configuration.getAcknowledgment() == Acknowledgment.Strategy.POST_PROCESSING) {
                            return Message.of(result, () -> input.ack());
                        } else {
                            return Message.of(result);
                        }
                    })
                    .buildRs();
        }
    }

    private void processMethodReturningACompletionStageOfMessageAndConsumingIndividualMessage() {
        this.processor = ReactiveStreams.<Message> builder()
                .flatMapCompletionStage(managePreProcessingAck())
                .flatMapCompletionStage(input -> {
                    CompletionStage<Message> cs = invoke(input);
                    return cs;
                })
                .buildRs();
    }

    private void processMethodReturningAUniOfMessageAndConsumingIndividualMessage() {
        this.processor = ReactiveStreams.<Message> builder()
                .flatMapCompletionStage(managePreProcessingAck())
                .flatMapCompletionStage(input -> {
                    Uni<Message> uni = invoke(input);
                    return uni.subscribeAsCompletionStage();
                })
                .buildRs();
    }

    private void processMethodReturningACompletionStageOfPayloadAndConsumingIndividualPayload() {
        this.processor = ReactiveStreams.<Message> builder()
                .flatMapCompletionStage(managePreProcessingAck())
                .<Message> flatMapCompletionStage(input -> {
                    CompletionStage<Object> cs = invoke(input.getPayload());
                    return cs
                            .thenApply(res -> Message.of(res, input.getMetadata(), () -> {
                                if (configuration.getAcknowledgment() == Acknowledgment.Strategy.POST_PROCESSING) {
                                    return input.ack();
                                } else {
                                    return CompletableFuture.<Void> completedFuture(null);
                                }
                            }));
                })
                .buildRs();
    }

    private void processMethodReturningAUniOfPayloadAndConsumingIndividualPayload() {
        this.processor = ReactiveStreams.<Message> builder()
                .flatMapCompletionStage(managePreProcessingAck())
                .<Message> flatMapCompletionStage(input -> {
                    Uni<Object> uni = invoke(input.getPayload());
                    return uni
                            .map(res -> Message.of(res, input.getMetadata(), () -> {
                                if (configuration.getAcknowledgment() == Acknowledgment.Strategy.POST_PROCESSING) {
                                    return input.ack();
                                } else {
                                    return CompletableFuture.<Void> completedFuture(null);
                                }
                            })).subscribeAsCompletionStage();
                })
                .buildRs();
    }

    private boolean isReturningAPublisherOrAPublisherBuilder() {
        Class<?> returnType = configuration.getReturnType();
        return ClassUtils.isAssignable(returnType, Publisher.class)
                || ClassUtils.isAssignable(returnType, PublisherBuilder.class);
    }

    private boolean isReturningAProcessorOrAProcessorBuilder() {
        Class<?> returnType = configuration.getReturnType();
        return ClassUtils.isAssignable(returnType, Processor.class)
                || ClassUtils.isAssignable(returnType, ProcessorBuilder.class);
    }
}
