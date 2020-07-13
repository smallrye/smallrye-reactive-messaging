package io.smallrye.reactive.messaging;

import static io.smallrye.reactive.messaging.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.i18n.ProviderLogging.log;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.helpers.ClassUtils;

public class SubscriberMediator extends AbstractMediator {

    private PublisherBuilder<? extends Message<?>> source;
    private SubscriberBuilder<Message<?>, Void> subscriber;
    /**
     * Keep track of the subscription to cancel it once the scope is terminated.
     */
    private final AtomicReference<Subscription> subscription = new AtomicReference<>();

    // Supported signatures:
    // 1. Subscriber<Message<I>> method()
    // 2. Subscriber<I> method()
    // 3. CompletionStage<?> method(Message<I> m) + Uni variant
    // 4. CompletionStage<?> method(I i) - + Uni variant
    // 5. void/? method(Message<I> m) - The support of this method has been removed (CES - Reactive Hangout 2018/09/11).
    // 6. void/? method(I i)

    public SubscriberMediator(MediatorConfiguration configuration) {
        super(configuration);
        if (configuration.shape() != Shape.SUBSCRIBER) {
            throw ex.illegalArgumentForSubscriberShape(configuration.shape());
        }
    }

    @Override
    public void initialize(Object bean) {
        super.initialize(bean);
        switch (configuration.consumption()) {
            case STREAM_OF_MESSAGE: // 1
            case STREAM_OF_PAYLOAD: // 2
                processMethodReturningASubscriber();
                break;
            case MESSAGE: // 3  (5 being dropped)
            case PAYLOAD: // 4 or 6
                if (ClassUtils.isAssignable(configuration.getReturnType(), CompletionStage.class)) {
                    // Case 3, 4
                    processMethodReturningACompletionStage();
                } else if (ClassUtils.isAssignable(configuration.getReturnType(), Uni.class)) {
                    // Case 3, 4 - Uni Variant
                    processMethodReturningAUni();
                } else {
                    // Case 6 (5 being dropped)
                    processMethodReturningVoid();
                }
                break;
            default:
                throw ex.illegalArgumentForUnexpectedConsumption(configuration.consumption());
        }

        assert this.subscriber != null;
    }

    @Override
    public SubscriberBuilder<Message<?>, Void> getComputedSubscriber() {
        return subscriber;
    }

    @Override
    public boolean isConnected() {
        return source != null;
    }

    @Override
    public void connectToUpstream(PublisherBuilder<? extends Message<?>> publisher) {
        this.source = publisher;
    }

    @SuppressWarnings({ "ReactiveStreamsSubscriberImplementation" })
    @Override
    public void run() {
        assert this.source != null;
        assert this.subscriber != null;

        AtomicReference<Throwable> syncErrorCatcher = new AtomicReference<>();
        Subscriber<Message<?>> delegate = this.subscriber.build();
        Subscriber<Message<?>> delegating = new Subscriber<Message<?>>() {
            @Override
            public void onSubscribe(Subscription s) {
                subscription.set(s);
                delegate.onSubscribe(s);
            }

            @Override
            public void onNext(Message<?> o) {
                delegate.onNext(o);
            }

            @Override
            public void onError(Throwable t) {
                log.streamProcessingException(t);
                syncErrorCatcher.set(t);
                delegate.onError(t);
            }

            @Override
            public void onComplete() {
                delegate.onComplete();
            }
        };

        this.source.to(delegating).run();
        // Check if a synchronous error has been caught
        Throwable throwable = syncErrorCatcher.get();
        if (throwable != null) {
            throw ex.weavingForIncoming(configuration.getIncoming(), throwable);
        }
    }

    private void processMethodReturningVoid() {
        if (configuration.isBlocking()) {
            this.subscriber = ReactiveStreams.<Message<?>> builder()
                    .flatMapCompletionStage(m -> Uni.createFrom().completionStage(handlePreProcessingAck(m))
                            .onItem().transformToUni(msg -> invokeBlocking(msg.getPayload()))
                            .onItemOrFailure().transformToUni(handleInvocationResult(m))
                            .subscribeAsCompletionStage())
                    .onError(failure -> health.report(configuration.getIncoming().toString(), failure))
                    .ignore();
        } else {
            this.subscriber = ReactiveStreams.<Message<?>> builder()
                    .flatMapCompletionStage(m -> Uni.createFrom().completionStage(handlePreProcessingAck(m))
                            .onItem().transform(msg -> invoke(msg.getPayload()))
                            .onItemOrFailure().transformToUni(handleInvocationResult(m))
                            .subscribeAsCompletionStage())
                    .onError(failure -> health.report(configuration.getIncoming().toString(), failure))
                    .ignore();
        }
    }

    private BiFunction<Object, Throwable, Uni<? extends Message<?>>> handleInvocationResult(
            Message<?> m) {
        return (success, failure) -> {
            if (failure != null) {
                if (configuration.getAcknowledgment() == Acknowledgment.Strategy.POST_PROCESSING) {
                    return Uni.createFrom().completionStage(m.nack(failure).thenApply(x -> m));
                } else {
                    // Invocation failed, but the message may have been already acknowledged (PRE or MANUAL), so
                    // we cannot nack. We propagate the failure downstream.
                    return Uni.createFrom().failure(failure);
                }
            } else {
                if (configuration.getAcknowledgment() == Acknowledgment.Strategy.POST_PROCESSING) {
                    return Uni.createFrom().completionStage(m.ack().thenApply(x -> m));
                } else {
                    return Uni.createFrom().item(m);
                }
            }
        };
    }

    private void processMethodReturningACompletionStage() {
        boolean invokeWithPayload = MediatorConfiguration.Consumption.PAYLOAD == configuration.consumption();
        this.subscriber = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(message -> Uni.createFrom().completionStage(handlePreProcessingAck(message))
                        .onItem().transformToUni(m -> {
                            CompletionStage<?> stage;
                            if (invokeWithPayload) {
                                stage = invoke(message.getPayload());
                            } else {
                                stage = invoke(message);
                            }
                            return Uni.createFrom().completionStage(stage.thenApply(x -> message));
                        })
                        .onItemOrFailure().transformToUni(handleInvocationResult(message))
                        .subscribeAsCompletionStage())
                .onError(failure -> health.report(configuration.getIncoming().toString(), failure))
                .ignore();
    }

    private void processMethodReturningAUni() {
        boolean invokeWithPayload = MediatorConfiguration.Consumption.PAYLOAD == configuration.consumption();

        this.subscriber = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(message -> Uni.createFrom().completionStage(handlePreProcessingAck(message))
                        .onItem().transformToUni(x -> {
                            if (invokeWithPayload) {
                                return invoke(message.getPayload());
                            } else {
                                return invoke(message);
                            }
                        })
                        .onItemOrFailure().transformToUni(handleInvocationResult(message))
                        .subscribeAsCompletionStage())
                .onError(failure -> health.report(configuration.getIncoming().toString(), failure))
                .ignore();
    }

    @SuppressWarnings("unchecked")
    private void processMethodReturningASubscriber() {
        Object result = invoke();
        if (!(result instanceof Subscriber) && !(result instanceof SubscriberBuilder)) {
            throw ex.illegalStateExceptionForSubscriberOrSubscriberBuilder(result.getClass().getName());
        }

        if (configuration.consumption() == MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD) {
            Subscriber<Object> sub;
            if (result instanceof Subscriber) {
                sub = (Subscriber<Object>) result;
            } else {
                sub = ((SubscriberBuilder<Object, Void>) result).build();
            }

            SubscriberWrapper<?, Message<?>> wrapper = new SubscriberWrapper<>(sub, Message::getPayload,
                    (m, t) -> {
                        if (configuration.getAcknowledgment() == Acknowledgment.Strategy.POST_PROCESSING) {
                            if (t != null) {
                                return m.nack(t);
                            } else {
                                return m.ack();
                            }
                        } else {
                            CompletableFuture<Void> future = new CompletableFuture<>();
                            if (t != null) {
                                future.completeExceptionally(t);
                            } else {
                                future.complete(null);
                            }
                            return future;
                        }
                    });
            this.subscriber = ReactiveStreams.<Message<?>> builder()
                    .flatMapCompletionStage(this::handlePreProcessingAck)
                    .via(wrapper)
                    .onError(failure -> health.report(configuration.getIncoming().toString(), failure))
                    .ignore();
        } else {
            Subscriber<Message<?>> sub;
            if (result instanceof Subscriber) {
                sub = (Subscriber<Message<?>>) result;
            } else {
                sub = ((SubscriberBuilder<Message<?>, Void>) result).build();
            }
            Subscriber<Message<?>> casted = sub;
            this.subscriber = ReactiveStreams.<Message<?>> builder()
                    .flatMapCompletionStage(this::handlePreProcessingAck)
                    .via(new SubscriberWrapper<>(casted, Function.identity(), null))
                    .onError(failure -> health.report(configuration.getIncoming().toString(), failure))
                    .ignore();
        }
    }
}
