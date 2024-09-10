package io.smallrye.reactive.messaging.providers;

import static io.smallrye.reactive.messaging.MediatorConfiguration.Production.STREAM_OF_MESSAGE;
import static io.smallrye.reactive.messaging.MediatorConfiguration.Production.STREAM_OF_PAYLOAD;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderMessages.msg;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.GenericPayload;
import io.smallrye.reactive.messaging.MediatorConfiguration;
import io.smallrye.reactive.messaging.Messages;
import io.smallrye.reactive.messaging.Shape;
import io.smallrye.reactive.messaging.providers.helpers.AcknowledgementCoordinator;
import io.smallrye.reactive.messaging.providers.helpers.ClassUtils;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import mutiny.zero.flow.adapters.AdaptersToFlow;

@SuppressWarnings("ReactiveStreamsUnusedPublisher")
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
        // 1.  Flow.Processor<Message<I>, Message<O>>, Processor<Message<I>, Message<O>>, ProcessorBuilder<Message<I>, Message<O>> method()
        // 2.  Flow.Processor<I, O>, Processor<I, O>, ProcessorBuilder<I, O> method()

        // 5.  Flow.Publisher<Message<O>>, Publisher<Message<O>>, PublisherBuilder<Message<O>> method(Message<I> msg)
        // 6.  Flow.Publisher<O>, Publisher<O>, PublisherBuilder<O> method(I payload)

        // 9. Message<O> method(Message<I> msg)
        // 10. O method(I payload)
        // 11. CompletionStage<O> method(I payload)
        // 12. CompletionStage<Message<O>> method(Message<I> msg)

        // IMPORTANT When returning a Publisher or a PublisherBuilder, you can't mix payloads and messages
        switch (configuration.production()) {
            case STREAM_OF_MESSAGE:
                // Case 1, 5
                if (isReturningAProcessorOrAReactiveStreamsProcessorOrAProcessorBuilder()) {
                    // Case 1
                    if (configuration.usesBuilderTypes()) {
                        processMethodReturningAProcessorBuilderOfMessages();
                    } else if (configuration.usesReactiveStreams()) {
                        processMethodReturningAReactiveStreamsProcessorOfMessages();
                    } else {
                        processMethodReturningAProcessorOfMessages();
                    }
                } else if (isReturningAPublisherOrAReactiveStreamsPublisherOrAPublisherBuilder()) {
                    // Case 5
                    if (configuration.usesBuilderTypes()) {
                        processMethodReturningAPublisherBuilderOfMessageAndConsumingMessages();
                    } else if (configuration.usesReactiveStreams()) {
                        processMethodReturningAReactiveStreamsPublisherOfMessageAndConsumingMessages();
                    } else {
                        processMethodReturningAPublisherOfMessageAndConsumingMessages();
                    }
                } else {
                    throw ex.illegalArgumentForInitialize(configuration.methodAsString());
                }
                break;
            case STREAM_OF_PAYLOAD:
                // Case 2, 6
                if (isReturningAProcessorOrAReactiveStreamsProcessorOrAProcessorBuilder()) {
                    // Case 2
                    if (configuration.usesBuilderTypes()) {
                        processMethodReturningAProcessorBuilderOfPayloads();
                    } else if (configuration.usesReactiveStreams()) {
                        processMethodReturningAReactiveStreamsProcessorOfPayloads();
                    } else {
                        processMethodReturningAProcessorOfPayloads();
                    }
                } else if (isReturningAPublisherOrAReactiveStreamsPublisherOrAPublisherBuilder()) {
                    // Case 6
                    if (configuration.usesBuilderTypes()) {
                        processMethodReturningAPublisherBuilderOfPayloadsAndConsumingPayloads();
                    } else if (configuration.usesReactiveStreams()) {
                        processMethodReturningAReactiveStreamsPublisherOfPayloadsAndConsumingPayloads();
                    } else {
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
                // Case 12
                processMethodReturningACompletionStageOfMessageAndConsumingIndividualItem();
                break;
            case COMPLETION_STAGE_OF_PAYLOAD:
                // Case 11
                processMethodReturningACompletionStageOfPayloadAndConsumingIndividualItem();
                break;
            case UNI_OF_MESSAGE:
                // Case 12 - Uni variant
                processMethodReturningAUniOfMessageAndConsumingIndividualItem();
                break;
            case UNI_OF_PAYLOAD:
                // Case 11 - Uni variant
                processMethodReturningAUniOfPayloadAndConsumingIndividualItem();
                break;
            default:
                throw ex.illegalArgumentForUnexpectedProduction(configuration.production());
        }
    }

    /**
     * {@code PublisherBuilder<Message<O>> method(Message<I> msg)}
     */
    @SuppressWarnings("unchecked")
    private void processMethodReturningAPublisherBuilderOfMessageAndConsumingMessages() {
        this.mapper = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                .onItem().transformToMultiAndConcatenate(
                        msg -> AdaptersToFlow.publisher(((PublisherBuilder<Message<?>>) invoke(msg)).buildRs()));
    }

    /**
     * {@code PublisherBuilder<Message<O>> method(Message<I> msg)}
     */
    @SuppressWarnings("unchecked")
    private void processMethodReturningAReactiveStreamsPublisherOfMessageAndConsumingMessages() {
        this.mapper = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                .onItem().transformToMultiAndConcatenate(
                        msg -> AdaptersToFlow.publisher((Publisher<Message<?>>) invoke(msg)));
    }

    /**
     * {@code Flow.Publisher<Message<O>> method(Message<I> msg)}
     */
    @SuppressWarnings("unchecked")
    private void processMethodReturningAPublisherOfMessageAndConsumingMessages() {
        this.mapper = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                .onItem().transformToMultiAndConcatenate(msg -> (Flow.Publisher<Message<?>>) invoke(msg));
    }

    /**
     * {@code ProcessorBuilder<Message<I>, Message<O>> method()}
     */
    private void processMethodReturningAProcessorBuilderOfMessages() {
        ProcessorBuilder<Message<?>, Message<?>> builder = Objects.requireNonNull(invoke(),
                msg.methodReturnedNull(configuration.methodAsString()));

        this.mapper = upstream -> {
            Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            return MultiUtils.via(multi, AdaptersToFlow.processor(builder.buildRs()));
        };
    }

    /**
     * {@code Processor<Message<I>, Message<O>> method()}
     */
    private void processMethodReturningAReactiveStreamsProcessorOfMessages() {
        Processor<Message<?>, Message<?>> result = Objects.requireNonNull(invoke(),
                msg.methodReturnedNull(configuration.methodAsString()));

        this.mapper = upstream -> {
            Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            return MultiUtils.via(multi, AdaptersToFlow.processor(result));
        };
    }

    /**
     * {@code Flow.Processor<Message<I>, Message<O>> method()}
     */
    private void processMethodReturningAProcessorOfMessages() {
        Flow.Processor<Message<?>, Message<?>> result = Objects.requireNonNull(invoke(),
                msg.methodReturnedNull(configuration.methodAsString()));

        this.mapper = upstream -> {
            Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            return MultiUtils.via(multi, result);
        };
    }

    /**
     * {@code ProcessorBuilder<O> method()}
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void processMethodReturningAProcessorBuilderOfPayloads() {
        ProcessorBuilder returnedProcessorBuilder = invoke();
        Objects.requireNonNull(returnedProcessorBuilder, msg.methodReturnedNull(configuration.methodAsString()));

        this.mapper = upstream -> {
            Multi<?> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                    .onItem().transform(Message::getPayload);
            return MultiUtils.via(multi, AdaptersToFlow.processor(returnedProcessorBuilder.buildRs()))
                    .onItem().transform(this::payloadToMessage);
        };
    }

    /**
     * {@code Processor<I, O> method()}
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void processMethodReturningAReactiveStreamsProcessorOfPayloads() {
        Processor returnedProcessor = invoke();
        Objects.requireNonNull(returnedProcessor, msg.methodReturnedNull(configuration.methodAsString()));
        this.mapper = upstream -> {
            Multi<?> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                    .onItem().transform(Message::getPayload);
            return MultiUtils.via(multi, AdaptersToFlow.processor(returnedProcessor))
                    .onItem().transform(this::payloadToMessage);
        };
    }

    /**
     * {@code Flow.Processor<I, O> method()}
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void processMethodReturningAProcessorOfPayloads() {
        Flow.Processor returnedProcessor = invoke();
        Objects.requireNonNull(returnedProcessor, msg.methodReturnedNull(configuration.methodAsString()));
        this.mapper = upstream -> {
            Multi<?> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                    .onItem().transform(Message::getPayload);
            return MultiUtils.via(multi, returnedProcessor)
                    .onItem().transform(this::payloadToMessage);
        };
    }

    /**
     * {@code PublisherBuilder<O> method(I payload)}
     */
    private void processMethodReturningAPublisherBuilderOfPayloadsAndConsumingPayloads() {
        this.mapper = upstream -> {
            Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            return multi.onItem().transformToMultiAndConcatenate(message -> {
                if (isPostAck()) {
                    try {
                        PublisherBuilder<?> pb = invoke(getArguments(message));
                        // POST_PROCESSING must not be used when returning an infinite stream
                        AcknowledgementCoordinator coordinator = new AcknowledgementCoordinator(message);
                        return MultiUtils.publisher(AdaptersToFlow.publisher(pb.buildRs()))
                                .onItem()
                                .transform(payload -> coordinator.track(payloadToMessage(payload, message.getMetadata())));
                    } catch (Throwable t) {
                        return handlePostInvocation(message, t);
                    }
                } else {
                    PublisherBuilder<?> pb = invoke(getArguments(message));
                    return MultiUtils.publisher(AdaptersToFlow.publisher(pb.buildRs()))
                            .onItem().transform(payload -> payloadToMessage(payload, message.getMetadata()));
                }
            });
        };
    }

    /**
     * {@code Publisher<O> method(I payload)}
     */
    private void processMethodReturningAReactiveStreamsPublisherOfPayloadsAndConsumingPayloads() {
        this.mapper = upstream -> {
            Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            return multi.onItem().transformToMultiAndConcatenate(message -> {
                if (isPostAck()) {
                    try {
                        Publisher<?> pub = invoke(getArguments(message));
                        // POST_PROCESSING must not be used when returning an infinite stream
                        AcknowledgementCoordinator coordinator = new AcknowledgementCoordinator(message);
                        return MultiUtils.publisher(AdaptersToFlow.publisher(pub))
                                .onItem()
                                .transform(payload -> coordinator.track(payloadToMessage(payload, message.getMetadata())));
                    } catch (Throwable t) {
                        return handlePostInvocation(message, t);
                    }
                } else {
                    Publisher<?> pub = invoke(getArguments(message));
                    return MultiUtils.publisher(AdaptersToFlow.publisher(pub))
                            .onItem().transform(payload -> payloadToMessage(payload, message.getMetadata()));
                }
            });
        };
    }

    /**
     * {@code Flow.Publisher<O> method(I payload)}
     */
    private void processMethodReturningAPublisherOfPayloadsAndConsumingPayloads() {
        this.mapper = upstream -> {
            Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            return multi.onItem().transformToMultiAndConcatenate(message -> {
                if (isPostAck()) {
                    try {
                        Flow.Publisher<?> pub = invoke(getArguments(message));
                        // POST_PROCESSING must not be used when returning an infinite stream
                        AcknowledgementCoordinator coordinator = new AcknowledgementCoordinator(message);
                        return MultiUtils.publisher(pub)
                                .onItem()
                                .transform(payload -> coordinator.track(payloadToMessage(payload, message.getMetadata())));

                    } catch (Throwable t) {
                        return handlePostInvocation(message, t);
                    }
                } else {
                    Flow.Publisher<?> pub = invoke(getArguments(message));
                    return MultiUtils.publisher(pub)
                            .onItem().transform(payload -> payloadToMessage(payload, message.getMetadata()));
                }
            });
        };
    }

    /**
     * {@code Message<O> method(Message<I> msg)}
     * {@code Message<O> method(I payload)}
     */
    private void processMethodReturningIndividualMessageAndConsumingIndividualItem() {
        // Item can be a message or a payload
        if (configuration.isBlocking()) {
            if (configuration.isBlockingExecutionOrdered()) {
                this.mapper = upstream -> {
                    Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
                    return multi
                            .onItem()
                            .transformToMultiAndConcatenate(message -> invokeBlocking(message, getArguments(message))
                                    .onItemOrFailure()
                                    .transformToUni((o, t) -> handlePostInvocationWithMessage(message, (Message<?>) o, t))
                                    .onItem().transformToMulti(this::handleSkip));
                };
            } else {
                this.mapper = upstream -> {
                    Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
                    return multi
                            .onItem().transformToMulti(message -> invokeBlocking(message, getArguments(message))
                                    .onItemOrFailure()
                                    .transformToUni((o, t) -> handlePostInvocationWithMessage(message, (Message<?>) o, t))
                                    .onItem().transformToMulti(this::handleSkip))
                            .merge(maxConcurrency());
                };
            }

        } else {
            this.mapper = upstream -> {
                Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
                return multi
                        .onItem().transformToMultiAndConcatenate(
                                message -> invokeOnMessageContext(message, getArguments(message))
                                        .onItem().transform(o -> (Message<?>) o)
                                        .onItemOrFailure()
                                        .transformToUni((r, f) -> handlePostInvocationWithMessage(message, r, f))
                                        .onItem().transformToMulti(this::handleSkip));
            };
        }
    }

    /**
     * {@code O method(I payload)}
     */
    private void processMethodReturningIndividualPayloadAndConsumingIndividualItem() {
        // Item can be message or payload.
        if (configuration.isBlocking()) {
            if (configuration.isBlockingExecutionOrdered()) {
                this.mapper = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                        .onItem()
                        .transformToMultiAndConcatenate(message -> invokeBlocking(message, getArguments(message))
                                .onItemOrFailure().transformToUni((r, f) -> handlePostInvocation(message, r, f))
                                .onItem().transformToMulti(this::handleSkip));
            } else {
                this.mapper = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                        .onItem().transformToMulti(message -> invokeBlocking(message, getArguments(message))
                                .onItemOrFailure().transformToUni((r, f) -> handlePostInvocation(message, r, f))
                                .onItem().transformToMulti(this::handleSkip))
                        .merge(maxConcurrency());
            }

        } else {
            this.mapper = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                    .onItem().transformToMultiAndConcatenate(
                            message -> invokeOnMessageContext(message, getArguments(message))
                                    .onItemOrFailure().transformToUni((r, f) -> handlePostInvocation(message, r, f))
                                    .onItem().transformToMulti(this::handleSkip));
        }
    }

    private Flow.Publisher<? extends Message<Object>> handleSkip(Message<Object> m) {
        if (m == null) { // If message is null, skip.
            return Multi.createFrom().empty();
        } else {
            return Multi.createFrom().item(m);
        }
    }

    private Multi<? extends Message<?>> handlePostInvocation(Message<?> message, Throwable fail) {
        return Uni.createFrom().completionStage(() -> message.nack(fail).thenApply(x -> (Message<?>) null)).toMulti();
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
            if (res instanceof GenericPayload) {
                GenericPayload<Object> genericPayload = (GenericPayload<Object>) res;
                if (isPostAck()) {
                    return Uni.createFrom().item(genericPayload.toMessage(message));
                } else {
                    return Uni.createFrom().item(Message.of(genericPayload.getPayload(),
                            Messages.merge(message.getMetadata(), genericPayload.getMetadata())));
                }
            }
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

    private Uni<? extends Message<Object>> handlePostInvocationWithMessage(Message<?> in, Message<?> res, Throwable fail) {
        if (fail != null) {
            if (isPostAck()) {
                // Here we nack the incoming, but maybe the message has already been (n)acked
                return Uni.createFrom()
                        .completionStage(in.nack(fail).thenApply(x -> null));
            } else {
                throw ex.processingException(getMethodAsString(), fail);
            }
        } else if (res != null) {
            if (isPostAck()) {
                // Here we chain the outgoing message to the incoming, but maybe the message has already been (n)acked
                return Uni.createFrom()
                        .item((Message<Object>) res.withAckWithMetadata(m -> res.ack(m).thenCompose(x -> in.ack(m)))
                                .withNackWithMetadata((t, m) -> res.nack(t, m).thenCompose(x -> in.nack(t, m))));
            }

            return Uni.createFrom().item((Message<Object>) res);
        } else {
            // the method returned null, the message is not forwarded
            if (isPostAck()) {
                // Here we ack the incoming message, but maybe the message has already been (n)acked
                return Uni.createFrom().completionStage(in.ack().thenApply(x -> null));
            }
            return Uni.createFrom().nullItem();
        }
    }

    /**
     * {@code CompletionStage<Message<O>> method(I payload)}
     */
    private void processMethodReturningACompletionStageOfMessageAndConsumingIndividualItem() {
        this.mapper = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                .onItem().transformToMultiAndConcatenate(
                        message -> invokeOnMessageContext(message, getArguments(message))
                                .onItem().transformToUni(cs -> Uni.createFrom().completionStage((CompletionStage<?>) cs))
                                .onItemOrFailure()
                                .transformToUni((r, f) -> handlePostInvocationWithMessage(message, (Message<?>) r, f))
                                .onItem().transformToMulti(this::handleSkip));
    }

    /**
     * {@code Uni<Message<O>> method(I payload)}
     */
    private void processMethodReturningAUniOfMessageAndConsumingIndividualItem() {
        this.mapper = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                .onItem().transformToMultiAndConcatenate(
                        message -> invokeOnMessageContext(message, getArguments(message))
                                .onItem().transformToUni(u -> (Uni<?>) u)
                                .onItemOrFailure()
                                .transformToUni((r, f) -> handlePostInvocationWithMessage(message, (Message<?>) r, f))
                                .onItem().transformToMulti(this::handleSkip));
    }

    /**
     * {@code CompletionStage<O> method(I payload)}
     */
    private void processMethodReturningACompletionStageOfPayloadAndConsumingIndividualItem() {
        this.mapper = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                .onItem().transformToMultiAndConcatenate(
                        message -> invokeOnMessageContext(message, getArguments(message))
                                .onItem().transformToUni(cs -> Uni.createFrom().completionStage((CompletionStage<?>) cs))
                                .onItemOrFailure().transformToUni((r, f) -> handlePostInvocation(message, r, f))
                                .onItem().transformToMulti(this::handleSkip));
    }

    /**
     * {@code Uni<O> method(I payload)}
     */
    private void processMethodReturningAUniOfPayloadAndConsumingIndividualItem() {
        this.mapper = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                .onItem().transformToMultiAndConcatenate(
                        message -> invokeOnMessageContext(message, getArguments(message))
                                .onItem().transformToUni(u -> (Uni<?>) u)
                                .onItemOrFailure().transformToUni((r, f) -> handlePostInvocation(message, r, f))
                                .onItem().transformToMulti(this::handleSkip));
    }

    private boolean isReturningAPublisherOrAReactiveStreamsPublisherOrAPublisherBuilder() {
        Class<?> returnType = configuration.getReturnType();
        return ClassUtils.isAssignable(returnType, Flow.Publisher.class)
                || ClassUtils.isAssignable(returnType, Publisher.class)
                || ClassUtils.isAssignable(returnType, PublisherBuilder.class);
    }

    private boolean isReturningAProcessorOrAReactiveStreamsProcessorOrAProcessorBuilder() {
        Class<?> returnType = configuration.getReturnType();
        return ClassUtils.isAssignable(returnType, Flow.Processor.class)
                || ClassUtils.isAssignable(returnType, Processor.class)
                || ClassUtils.isAssignable(returnType, ProcessorBuilder.class);
    }

    private boolean isPostAck() {
        return configuration.getAcknowledgment() == Acknowledgment.Strategy.POST_PROCESSING;
    }

}
