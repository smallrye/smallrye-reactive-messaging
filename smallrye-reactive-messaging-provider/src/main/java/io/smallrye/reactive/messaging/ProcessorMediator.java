package io.smallrye.reactive.messaging;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.helpers.ClassUtils;

public class ProcessorMediator extends AbstractMediator {

    private Processor<Message<?>, ? extends Message<?>> processor;
    private PublisherBuilder<? extends Message<?>> publisher;

    public ProcessorMediator(MediatorConfiguration configuration) {
        super(configuration);
        if (configuration.shape() != Shape.PROCESSOR) {
            throw new IllegalArgumentException("Expected a Processor shape, received a " + configuration.shape());
        }
    }

    @Override
    public void connectToUpstream(PublisherBuilder<? extends Message<?>> publisher) {
        assert processor != null;
        this.publisher = decorate(publisher.via(processor));
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getStream() {
        return Objects.requireNonNull(publisher);
    }

    @Override
    public boolean isConnected() {
        return publisher != null;
    }

    @Override
    protected <T> Uni<T> invokeBlocking(Object... args) {
        return super.invokeBlocking(args);
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

    @SuppressWarnings("unchecked")
    private void processMethodReturningAPublisherBuilderOfMessageAndConsumingMessages() {
        this.processor = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(managePreProcessingAck())
                .map(msg -> (PublisherBuilder<Message<?>>) invoke(msg))
                .flatMap(Function.identity())
                .buildRs();
    }

    @SuppressWarnings("unchecked")
    private void processMethodReturningAPublisherOfMessageAndConsumingMessages() {
        this.processor = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(managePreProcessingAck())
                .map(msg -> (Publisher<Message<?>>) invoke(msg))
                .flatMapRsPublisher(Function.identity())
                .buildRs();
    }

    private void processMethodReturningAProcessorBuilderOfMessages() {
        ProcessorBuilder<Message<?>, Message<?>> builder = Objects.requireNonNull(invoke(),
                "The method " + configuration.methodAsString() + " returned `null`");

        this.processor = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(managePreProcessingAck())
                .via(builder)
                .buildRs();
    }

    private void processMethodReturningAProcessorOfMessages() {
        Processor<Message<?>, Message<?>> result = Objects.requireNonNull(invoke(),
                "The method " + configuration.methodAsString() + " returned `null`");
        this.processor = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(msg -> {
                    if (configuration.getAcknowledgment() == Acknowledgment.Strategy.PRE_PROCESSING) {
                        return msg.ack().thenApply((x -> msg));
                    } else {
                        return CompletableFuture.completedFuture(msg);
                    }
                })
                .via(result)
                .buildRs();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void processMethodReturningAProcessorOfPayloads() {
        Processor returnedProcessor = invoke();

        this.processor = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(managePreProcessingAck())
                .map(Message::getPayload)
                .via(returnedProcessor)
                .map(Message::of)
                .buildRs();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void processMethodReturningAProcessorBuilderOfPayloads() {
        ProcessorBuilder returnedProcessorBuilder = invoke();
        Objects.requireNonNull(returnedProcessorBuilder, "The method " + configuration.methodAsString()
                + " has returned an invalid value: null");

        this.processor = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(managePreProcessingAck())
                .map(Message::getPayload)
                .via(returnedProcessorBuilder)
                .map(Message::of)
                .buildRs();
    }

    private void processMethodReturningAPublisherBuilderOfPayloadsAndConsumingPayloads() {
        this.processor = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(managePreProcessingAck())
                .flatMap(message -> {
                    PublisherBuilder<?> pb = invoke(message.getPayload());
                    return pb.map(payload -> Message.of(payload, message.getMetadata()));
                    // TODO We can handle post-acknowledgement here.
                })
                .buildRs();
    }

    private void processMethodReturningAPublisherOfPayloadsAndConsumingPayloads() {
        this.processor = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(managePreProcessingAck())
                .flatMap(message -> {
                    Publisher<?> pub = invoke(message.getPayload());
                    return ReactiveStreams.fromPublisher(pub)
                            .map(payload -> Message.of(payload, message.getMetadata()));
                    // TODO We can handle post-acknowledgement here.
                })
                .buildRs();
    }

    private void processMethodReturningIndividualMessageAndConsumingIndividualItem() {
        // Item can be message or payload
        if (configuration.consumption() == MediatorConfiguration.Consumption.PAYLOAD) {
            if (configuration.isBlocking()) {
                this.processor = ReactiveStreams.<Message<?>> builder()
                        .flatMapRsPublisher(message -> Uni.createFrom().completionStage(handlePreProcessingAck(message))
                                .onItem().produceUni(x -> invokeBlocking(message.getPayload()))
                                .onItem().apply(x -> (Message<?>) x)
                                .onItemOrFailure().<Message<Object>> produceUni(this::handlePostInvocationWithMessage)
                                .onItem().produceMulti(this::handleSkip))
                        .buildRs();
            } else {
                this.processor = ReactiveStreams.<Message<?>> builder()
                        .flatMapRsPublisher(message -> Uni.createFrom().completionStage(handlePreProcessingAck(message))
                                .onItem().apply(x -> invoke(message.getPayload()))
                                .onItem().apply(x -> (Message<?>) x)
                                .onItemOrFailure().<Message<Object>> produceUni(this::handlePostInvocationWithMessage)
                                .onItem().produceMulti(this::handleSkip))
                        .buildRs();
            }
        } else {
            if (configuration.isBlocking()) {
                this.processor = ReactiveStreams.<Message<?>> builder()
                        .flatMapRsPublisher(message -> Uni.createFrom().completionStage(handlePreProcessingAck(message))
                                .onItem().produceUni(x -> invokeBlocking(message))
                                .onItem().apply(x -> (Message<?>) x)
                                .onItemOrFailure().<Message<Object>> produceUni(this::handlePostInvocationWithMessage)
                                .onItem().produceMulti(this::handleSkip))
                        .buildRs();
            } else {
                this.processor = ReactiveStreams.<Message<?>> builder()
                        .flatMapRsPublisher(message -> Uni.createFrom().completionStage(handlePreProcessingAck(message))
                                .onItem().apply(x -> invoke(message))
                                .onItem().apply(x -> (Message<?>) x)
                                .onItemOrFailure().<Message<Object>> produceUni(this::handlePostInvocationWithMessage)
                                .onItem().produceMulti(this::handleSkip))
                        .buildRs();
            }
        }
    }

    private boolean isPostAck() {
        return configuration.getAcknowledgment() == Acknowledgment.Strategy.POST_PROCESSING;
    }

    private void processMethodReturningIndividualPayloadAndConsumingIndividualItem() {
        // Item can be message or payload.
        if (configuration.consumption() == MediatorConfiguration.Consumption.PAYLOAD) {
            if (configuration.isBlocking()) {
                this.processor = ReactiveStreams.<Message<?>> builder()
                        .flatMapRsPublisher(message -> Uni.createFrom().completionStage(handlePreProcessingAck(message))
                                .onItem().produceUni(x -> (Uni<?>) invokeBlocking(message.getPayload()))
                                .onItemOrFailure()
                                .<Message<Object>> produceUni((res, fail) -> handlePostInvocation(message, res, fail))
                                .onItem().produceMulti(this::handleSkip))
                        .buildRs();
            } else {
                this.processor = ReactiveStreams.<Message<?>> builder()
                        .flatMapRsPublisher(message -> Uni.createFrom().completionStage(handlePreProcessingAck(message))
                                .onItem().apply(input -> invoke(input.getPayload()))
                                .onItemOrFailure()
                                .<Message<Object>> produceUni((res, fail) -> handlePostInvocation(message, res, fail))
                                .onItem().produceMulti(this::handleSkip))
                        .buildRs();
            }
        } else {
            // Method consuming message and producing payloads
            if (configuration.isBlocking()) {
                this.processor = ReactiveStreams.<Message<?>> builder()
                        .flatMapRsPublisher(message -> Uni.createFrom().completionStage(handlePreProcessingAck(message))
                                .onItem().produceUni(x -> (Uni<?>) invokeBlocking(message))
                                .onItemOrFailure()
                                .<Message<Object>> produceUni((res, fail) -> handlePostInvocation(message, res, fail))
                                .onItem().produceMulti(this::handleSkip))
                        .buildRs();
            } else {
                this.processor = ReactiveStreams.<Message<?>> builder()
                        .flatMapRsPublisher(message -> Uni.createFrom().completionStage(handlePreProcessingAck(message))
                                .onItem().apply(this::invoke)
                                .onItemOrFailure()
                                .<Message<Object>> produceUni((res, fail) -> handlePostInvocation(message, res, fail))
                                .onItem().produceMulti(this::handleSkip))
                        .buildRs();
            }
        }
    }

    private Publisher<? extends Message<Object>> handleSkip(Message<Object> m) {
        if (m == null) { // If message is null, skip.
            return Multi.createFrom().empty();
        } else {
            return Multi.createFrom().item(m);
        }
    }

    private Uni<? extends Message<Object>> handlePostInvocation(Message<?> message, Object res, Throwable fail) {
        if (fail != null) {
            if (isPostAck()) {
                return Uni.createFrom()
                        .completionStage(message.nack(fail).thenApply(x -> (Message<Object>) null));
            } else {
                throw new ProcessingException(getMethodAsString(), fail);
            }
        } else if (res != null) {
            if (isPostAck()) {
                return Uni.createFrom().item(message.withPayload(res));
            } else {
                return Uni.createFrom().item(Message.of(res, message.getMetadata()));
            }
        } else {
            // the method returned null, the message is not forwarded, but we ack the message in post ack
            if (isPostAck()) {
                return Uni.createFrom()
                        .completionStage(message.ack().thenApply(x -> (Message<Object>) null));
            } else {
                return Uni.createFrom().nullItem();
            }
        }
    }

    private Uni<? extends Message<Object>> handlePostInvocationWithMessage(Message<?> res,
            Throwable fail) {
        if (fail != null) {
            throw new ProcessingException(getMethodAsString(), fail);
        } else if (res != null) {
            return Uni.createFrom().item((Message<Object>) res);
        } else {
            // the method returned null, the message is not forwarded
            return Uni.createFrom().nullItem();
        }
    }

    private void processMethodReturningACompletionStageOfMessageAndConsumingIndividualMessage() {
        this.processor = ReactiveStreams.<Message<?>> builder()
                .flatMapRsPublisher(message -> Uni.createFrom().completionStage(handlePreProcessingAck(message))
                        .onItem().produceCompletionStage(x -> invoke(message))
                        .onItemOrFailure().produceUni((res, fail) -> handlePostInvocationWithMessage((Message) res, fail))
                        .onItem().produceMulti(this::handleSkip))
                .buildRs();
    }

    private void processMethodReturningAUniOfMessageAndConsumingIndividualMessage() {
        this.processor = ReactiveStreams.<Message<?>> builder()
                .flatMapRsPublisher(message -> Uni.createFrom().completionStage(handlePreProcessingAck(message))
                        .onItem().produceUni(x -> invoke(message))
                        .onItemOrFailure().produceUni((res, fail) -> handlePostInvocationWithMessage((Message) res, fail))
                        .onItem().produceMulti(this::handleSkip))
                .buildRs();
    }

    private void processMethodReturningACompletionStageOfPayloadAndConsumingIndividualPayload() {
        this.processor = ReactiveStreams.<Message<?>> builder()
                .flatMapRsPublisher(message -> Uni.createFrom().completionStage(handlePreProcessingAck(message))
                        .onItem().produceCompletionStage(x -> invoke(message.getPayload()))
                        .onItemOrFailure().produceUni((res, fail) -> handlePostInvocation(message, res, fail))
                        .onItem().produceMulti(this::handleSkip))
                .buildRs();
    }

    private void processMethodReturningAUniOfPayloadAndConsumingIndividualPayload() {
        this.processor = ReactiveStreams.<Message<?>> builder()
                .flatMapRsPublisher(message -> Uni.createFrom().completionStage(handlePreProcessingAck(message))
                        .onItem().produceUni(x -> invoke(message.getPayload()))
                        .onItemOrFailure().produceUni((res, fail) -> handlePostInvocation(message, res, fail))
                        .onItem().produceMulti(this::handleSkip))
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
