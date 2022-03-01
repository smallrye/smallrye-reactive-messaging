package io.smallrye.reactive.messaging.providers;

import static io.smallrye.reactive.messaging.MediatorConfiguration.Production.STREAM_OF_MESSAGE;
import static io.smallrye.reactive.messaging.MediatorConfiguration.Production.STREAM_OF_PAYLOAD;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderMessages.msg;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
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
import io.smallrye.reactive.messaging.MediatorConfiguration;
import io.smallrye.reactive.messaging.Shape;
import io.smallrye.reactive.messaging.providers.helpers.ClassUtils;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;

public class ProcessorMediator extends AbstractMediator {

    private Function<Multi<? extends Message<?>>, Multi<? extends Message<?>>> mapper;
    private Multi<? extends Message<?>> publisher;

    public ProcessorMediator(MediatorConfiguration configuration) {
        super(configuration);
        if (configuration.shape() != Shape.PROCESSOR) {
            throw ex.illegalArgumentForProcessorShape(configuration.shape());
        }

        // IMPORTANT When returning a Multi, Publisher or a PublisherBuilder, you can't mix payloads and messages.
        if (configuration.production() == STREAM_OF_MESSAGE
                && configuration.consumption() == MediatorConfiguration.Consumption.PAYLOAD) {
            throw ex.definitionProduceMessageStreamAndConsumePayload(configuration.methodAsString());
        }

        if (configuration.production() == STREAM_OF_PAYLOAD
                && configuration.consumption() == MediatorConfiguration.Consumption.MESSAGE) {
            throw ex.definitionProducePayloadStreamAndConsumeMessage(configuration.methodAsString());
        }
    }

    @Override
    public void connectToUpstream(Multi<? extends Message<?>> publisher) {
        assert mapper != null;
        this.publisher = decorate(publisher.plug(m -> mapper.apply(convert(m))));
    }

    @Override
    public Multi<? extends Message<?>> getStream() {
        return Objects.requireNonNull(publisher);
    }

    @Override
    public boolean isConnected() {
        return publisher != null;
    }

    @Override
    protected <T> Uni<T> invokeBlocking(Message<?> message, Object... args) {
        return super.invokeBlocking(message, args);
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

        // IMPORTANT When returning a Publisher or a PublisherBuilder, you can't mix payloads and messages
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
                    throw ex.illegalArgumentForInitialize(configuration.methodAsString());
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
                    throw ex.illegalArgumentForInitialize(configuration.methodAsString());
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
                processMethodReturningACompletionStageOfMessageAndConsumingIndividualItem();
                break;
            case COMPLETION_STAGE_OF_PAYLOAD:
                // Case 12
                processMethodReturningACompletionStageOfPayloadAndConsumingIndividualItem();
                break;
            case UNI_OF_MESSAGE:
                // Case 11 - Uni variant
                processMethodReturningAUniOfMessageAndConsumingIndividualItem();
                break;
            case UNI_OF_PAYLOAD:
                // Case 12 - Uni variant
                processMethodReturningAUniOfPayloadAndConsumingIndividualItem();
                break;
            default:
                throw ex.illegalArgumentForUnexpectedProduction(configuration.production());
        }
    }

    @SuppressWarnings("unchecked")
    private void processMethodReturningAPublisherBuilderOfMessageAndConsumingMessages() {
        this.mapper = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                .onItem().transformToMultiAndConcatenate(msg -> ((PublisherBuilder<Message<?>>) invoke(msg)).buildRs());
    }

    @SuppressWarnings("unchecked")
    private void processMethodReturningAPublisherOfMessageAndConsumingMessages() {
        this.mapper = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                .onItem().transformToMultiAndConcatenate(msg -> ((Publisher<Message<?>>) invoke(msg)));
    }

    private void processMethodReturningAProcessorBuilderOfMessages() {
        ProcessorBuilder<Message<?>, Message<?>> builder = Objects.requireNonNull(invoke(),
                msg.methodReturnedNull(configuration.methodAsString()));

        this.mapper = upstream -> {
            Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            // TODO Add a via to MultiUtils
            return Multi.createFrom().publisher(ReactiveStreams.fromPublisher(multi).via(builder).buildRs());
        };
    }

    private void processMethodReturningAProcessorOfMessages() {
        Processor<Message<?>, Message<?>> result = Objects.requireNonNull(invoke(),
                msg.methodReturnedNull(configuration.methodAsString()));

        this.mapper = upstream -> {
            Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            // TODO Add a via to MultiUtils
            return Multi.createFrom().publisher(ReactiveStreams.fromPublisher(multi).via(result).buildRs());
        };
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void processMethodReturningAProcessorOfPayloads() {
        Processor returnedProcessor = invoke();
        Objects.requireNonNull(returnedProcessor, msg.methodReturnedNull(configuration.methodAsString()));
        this.mapper = upstream -> {
            Multi<?> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                    .onItem().transform(Message::getPayload);
            // TODO Add a via to MultiUtils
            return Multi.createFrom().publisher(ReactiveStreams.fromPublisher(multi).via(returnedProcessor).buildRs())
                    .onItem().transform(Message::of);
        };
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void processMethodReturningAProcessorBuilderOfPayloads() {
        ProcessorBuilder returnedProcessorBuilder = invoke();
        Objects.requireNonNull(returnedProcessorBuilder, msg.methodReturnedNull(configuration.methodAsString()));

        this.mapper = upstream -> {
            Multi<?> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                    .onItem().transform(Message::getPayload);
            // TODO Add a via to MultiUtils
            return Multi.createFrom().publisher(ReactiveStreams.fromPublisher(multi).via(returnedProcessorBuilder).buildRs())
                    .onItem().transform(Message::of);
        };
    }

    private void processMethodReturningAPublisherBuilderOfPayloadsAndConsumingPayloads() {
        this.mapper = upstream -> {
            Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            return multi.onItem().transformToMultiAndConcatenate(message -> {
                PublisherBuilder<?> pb = invoke(message.getPayload());
                return Multi.createFrom().publisher(pb.buildRs())
                        .onItem().transform(payload -> Message.of(payload, message.getMetadata()));
                // TODO We can handle post-acknowledgement here. -> onCompletion
            });
        };
    }

    private void processMethodReturningAPublisherOfPayloadsAndConsumingPayloads() {
        this.mapper = upstream -> {
            Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            return multi.onItem().transformToMultiAndConcatenate(message -> {
                Publisher<?> pub = invoke(message.getPayload());
                return Multi.createFrom().publisher(pub)
                        .onItem().transform(payload -> Message.of(payload, message.getMetadata()));
                // TODO We can handle post-acknowledgement here. -> onCompletion
            });
        };
    }

    private void processMethodReturningIndividualMessageAndConsumingIndividualItem() {
        // Item can be a message or a payload
        if (configuration.isBlocking()) {
            if (configuration.isBlockingExecutionOrdered()) {
                this.mapper = upstream -> {
                    Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
                    return multi
                            .onItem()
                            .transformToMultiAndConcatenate(message -> invokeBlocking(message, withPayloadOrMessage(message))
                                    .onItemOrFailure()
                                    .transformToUni((o, t) -> this.handlePostInvocationWithMessage((Message<?>) o, t))
                                    .onItem().transformToMulti(this::handleSkip));
                };
            } else {
                this.mapper = upstream -> {
                    Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
                    return multi
                            .onItem().transformToMultiAndMerge(message -> invokeBlocking(message, withPayloadOrMessage(message))
                                    .onItemOrFailure()
                                    .transformToUni((o, t) -> this.handlePostInvocationWithMessage((Message<?>) o, t))
                                    .onItem().transformToMulti(this::handleSkip));
                };
            }

        } else {
            this.mapper = upstream -> {
                Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
                return multi
                        .onItem().transformToMultiAndConcatenate(
                                message -> invokeOnMessageContext(message, withPayloadOrMessage(message))
                                        .onItem().transform(o -> (Message<?>) o)
                                        .onItemOrFailure().transformToUni(this::handlePostInvocationWithMessage)
                                        .onItem().transformToMulti(this::handleSkip));
            };
        }
    }

    private boolean isPostAck() {
        return configuration.getAcknowledgment() == Acknowledgment.Strategy.POST_PROCESSING;
    }

    private void processMethodReturningIndividualPayloadAndConsumingIndividualItem() {
        // Item can be message or payload.
        if (configuration.isBlocking()) {
            if (configuration.isBlockingExecutionOrdered()) {
                this.mapper = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                        .onItem()
                        .transformToMultiAndConcatenate(message -> invokeBlocking(message, withPayloadOrMessage(message))
                                .onItemOrFailure().transformToUni((r, f) -> handlePostInvocation(message, r, f))
                                .onItem().transformToMulti(this::handleSkip));
            } else {
                this.mapper = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                        .onItem().transformToMultiAndMerge(message -> invokeBlocking(message, withPayloadOrMessage(message))
                                .onItemOrFailure().transformToUni((r, f) -> handlePostInvocation(message, r, f))
                                .onItem().transformToMulti(this::handleSkip));
            }

        } else {
            this.mapper = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                    .onItem().transformToMultiAndConcatenate(
                            message -> invokeOnMessageContext(message, withPayloadOrMessage(message))
                                    .onItemOrFailure().transformToUni((r, f) -> handlePostInvocation(message, r, f))
                                    .onItem().transformToMulti(this::handleSkip));
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
                        .completionStage(message.nack(fail).thenApply(x -> null));
            } else {
                throw ex.processingException(getMethodAsString(), fail);
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
                        .completionStage(message.ack().thenApply(x -> null));
            } else {
                return Uni.createFrom().nullItem();
            }
        }
    }

    private Uni<? extends Message<Object>> handlePostInvocationWithMessage(Message<?> res,
            Throwable fail) {
        if (fail != null) {
            throw ex.processingException(getMethodAsString(), fail);
        } else if (res != null) {
            return Uni.createFrom().item((Message<Object>) res);
        } else {
            // the method returned null, the message is not forwarded
            return Uni.createFrom().nullItem();
        }
    }

    private void processMethodReturningACompletionStageOfMessageAndConsumingIndividualItem() {
        this.mapper = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                .onItem().transformToMultiAndConcatenate(
                        message -> invokeOnMessageContext(message, withPayloadOrMessage(message))
                                .onItem().transformToUni(cs -> Uni.createFrom().completionStage((CompletionStage<?>) cs))
                                .onItemOrFailure().transformToUni((r, f) -> handlePostInvocationWithMessage((Message<?>) r, f))
                                .onItem().transformToMulti(this::handleSkip));
    }

    private void processMethodReturningAUniOfMessageAndConsumingIndividualItem() {
        this.mapper = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                .onItem().transformToMultiAndConcatenate(
                        message -> invokeOnMessageContext(message, withPayloadOrMessage(message))
                                .onItem().transformToUni(u -> (Uni<?>) u)
                                .onItemOrFailure().transformToUni((r, f) -> handlePostInvocationWithMessage((Message<?>) r, f))
                                .onItem().transformToMulti(this::handleSkip));
    }

    private void processMethodReturningACompletionStageOfPayloadAndConsumingIndividualItem() {
        this.mapper = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                .onItem().transformToMultiAndConcatenate(
                        message -> invokeOnMessageContext(message, withPayloadOrMessage(message))
                                .onItem().transformToUni(cs -> Uni.createFrom().completionStage((CompletionStage<?>) cs))
                                .onItemOrFailure().transformToUni((r, f) -> handlePostInvocation(message, r, f))
                                .onItem().transformToMulti(this::handleSkip));
    }

    private void processMethodReturningAUniOfPayloadAndConsumingIndividualItem() {
        this.mapper = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                .onItem().transformToMultiAndConcatenate(
                        message -> invokeOnMessageContext(message, withPayloadOrMessage(message))
                                .onItem().transformToUni(u -> (Uni<?>) u)
                                .onItemOrFailure().transformToUni((r, f) -> handlePostInvocation(message, r, f))
                                .onItem().transformToMulti(this::handleSkip));
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

    private Object withPayloadOrMessage(Message<?> message) {
        return (configuration.consumption() == MediatorConfiguration.Consumption.PAYLOAD) ? message.getPayload() : message;
    }
}
