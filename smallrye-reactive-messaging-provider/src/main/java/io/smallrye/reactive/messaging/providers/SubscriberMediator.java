package io.smallrye.reactive.messaging.providers;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderLogging.log;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.reactive.messaging.MediatorConfiguration;
import io.smallrye.reactive.messaging.Shape;
import io.smallrye.reactive.messaging.providers.helpers.ClassUtils;
import io.smallrye.reactive.messaging.providers.helpers.IgnoringSubscriber;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import mutiny.zero.flow.adapters.AdaptersToFlow;

public class SubscriberMediator extends AbstractMediator {

    private Multi<? extends Message<?>> source;
    private Flow.Subscriber<Message<?>> subscriber;

    private Function<Multi<? extends Message<?>>, Multi<? extends Message<?>>> function;

    /**
     * Keep track of the subscription to cancel it once the scope is terminated.
     */
    private final AtomicReference<Flow.Subscription> subscription = new AtomicReference<>();

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
    public Flow.Subscriber<Message<?>> getComputedSubscriber() {
        return subscriber;
    }

    @Override
    public boolean isConnected() {
        return source != null;
    }

    @Override
    public void connectToUpstream(Multi<? extends Message<?>> publisher) {
        this.source = convert(publisher);
    }

    @SuppressWarnings({ "ReactiveStreamsSubscriberImplementation" })
    @Override
    public void run() {
        assert this.source != null;
        assert this.function != null;
        assert this.subscriber != null;

        AtomicReference<Throwable> syncErrorCatcher = new AtomicReference<>();
        Flow.Subscriber<Message<?>> delegate = this.subscriber;
        Flow.Subscriber<Message<?>> delegating = new MultiSubscriber<Message<?>>() {
            @Override
            public void onSubscribe(Flow.Subscription s) {
                subscription.set(s);
                delegate.onSubscribe(new Flow.Subscription() {
                    @Override
                    public void request(long n) {
                        s.request(n);
                    }

                    @Override
                    public void cancel() {
                        s.cancel();
                    }
                });
            }

            @Override
            public void onItem(Message<?> item) {
                try {
                    delegate.onNext(item);
                } catch (Exception e) {
                    log.messageProcessingException(configuration.methodAsString(), e);
                    syncErrorCatcher.set(e);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                log.messageProcessingException(configuration.methodAsString(), t);
                syncErrorCatcher.set(t);
                delegate.onError(t);
            }

            @Override
            public void onCompletion() {
                delegate.onComplete();
            }
        };

        Multi<? extends Message<?>> subscriberSource = decorateSubscriberSource(this.source);
        function.apply(subscriberSource).subscribe(delegating);
        // Check if a synchronous error has been caught
        Throwable throwable = syncErrorCatcher.get();
        if (throwable != null) {
            throw ex.weavingForIncoming(configuration.getIncoming(), throwable);
        }
    }

    private void processMethodReturningVoid() {
        this.subscriber = IgnoringSubscriber.INSTANCE;
        if (configuration.isBlocking()) {
            if (configuration.isBlockingExecutionOrdered()) {
                this.function = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                        .onItem().transformToUniAndConcatenate(msg -> invokeBlocking(msg, msg.getPayload())
                                .onItemOrFailure().transformToUni(handleInvocationResult(msg)))
                        .onFailure()
                        .invoke(failure -> health.reportApplicationFailure(configuration.methodAsString(), failure));
            } else {
                this.function = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                        .onItem().transformToUniAndMerge(msg -> invokeBlocking(msg, msg.getPayload())
                                .onItemOrFailure().transformToUni(handleInvocationResult(msg)))
                        .onFailure()
                        .invoke(failure -> health.reportApplicationFailure(configuration.methodAsString(), failure));
            }
        } else {
            this.function = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                    .onItem()
                    .transformToUniAndConcatenate(
                            msg -> invokeOnMessageContext(msg, msg.getPayload())
                                    .onItemOrFailure().transformToUni(handleInvocationResult(msg)))
                    .onFailure().invoke(failure -> health.reportApplicationFailure(configuration.methodAsString(), failure));
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
        this.subscriber = IgnoringSubscriber.INSTANCE;
        boolean invokeWithPayload = MediatorConfiguration.Consumption.PAYLOAD == configuration.consumption();
        if (configuration.isBlocking()) {
            if (configuration.isBlockingExecutionOrdered()) {
                this.function = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                        .onItem().transformToUniAndConcatenate(msg -> invokeBlockingAndHandleOutcome(invokeWithPayload, msg))
                        .onFailure().invoke(this::reportFailure);
            } else {
                this.function = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                        .onItem().transformToUniAndMerge(msg -> invokeBlockingAndHandleOutcome(invokeWithPayload, msg))
                        .onFailure().invoke(this::reportFailure);
            }
        } else {
            this.function = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                    .onItem().transformToUniAndConcatenate(msg -> {
                        Uni<?> uni;
                        if (invokeWithPayload) {
                            uni = invokeOnMessageContext(msg, msg.getPayload())
                                    .onItem().transformToUni(cs -> Uni.createFrom().completionStage((CompletionStage<?>) cs));
                        } else {
                            uni = invokeOnMessageContext(msg, msg)
                                    .onItem().transformToUni(cs -> Uni.createFrom().completionStage((CompletionStage<?>) cs));
                        }
                        return uni.onItemOrFailure().transformToUni(handleInvocationResult(msg));
                    })
                    .onFailure().invoke(this::reportFailure);
        }
    }

    private Uni<? extends Message<?>> invokeBlockingAndHandleOutcome(boolean invokeWithPayload, Message<?> msg) {
        Uni<?> uni;
        if (invokeWithPayload) {
            uni = invokeBlocking(msg, msg.getPayload());
        } else {
            uni = invokeBlocking(msg, msg);
        }
        return uni.onItemOrFailure().transformToUni(handleInvocationResult(msg));
    }

    private void reportFailure(Throwable failure) {
        log.messageProcessingException(configuration.methodAsString(), failure);
        health.reportApplicationFailure(configuration.methodAsString(), failure);
    }

    private void processMethodReturningAUni() {
        this.subscriber = IgnoringSubscriber.INSTANCE;
        boolean invokeWithPayload = MediatorConfiguration.Consumption.PAYLOAD == configuration.consumption();
        if (configuration.isBlocking()) {
            if (configuration.isBlockingExecutionOrdered()) {
                this.function = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                        .onItem().transformToUniAndConcatenate(msg -> invokeBlockingAndHandleOutcome(invokeWithPayload, msg))
                        .onFailure().invoke(this::reportFailure);
            } else {
                this.function = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                        .onItem().transformToUniAndMerge(msg -> invokeBlockingAndHandleOutcome(invokeWithPayload, msg))
                        .onFailure().invoke(this::reportFailure);
            }
        } else {
            this.function = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                    .onItem().transformToUniAndConcatenate(msg -> {
                        Uni<?> uni;
                        if (invokeWithPayload) {
                            uni = invokeOnMessageContext(msg, msg.getPayload())
                                    .onItem().transformToUni(u -> (Uni<?>) u);
                        } else {
                            uni = invokeOnMessageContext(msg, msg)
                                    .onItem().transformToUni(u -> (Uni<?>) u);
                        }
                        return uni.onItemOrFailure().transformToUni(handleInvocationResult(msg));
                    })
                    .onFailure().invoke(this::reportFailure);
        }
    }

    @SuppressWarnings("unchecked")
    private void processMethodReturningASubscriber() {
        Object result = invoke();
        if (!(result instanceof Flow.Subscriber) && !(result instanceof SubscriberBuilder) && !(result instanceof Subscriber)) {
            throw ex.illegalStateExceptionForSubscriberOrSubscriberBuilder(result.getClass().getName());
        }

        if (configuration.consumption() == MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD) {
            Flow.Subscriber<Object> userSubscriber;
            if (result instanceof Flow.Subscriber) {
                userSubscriber = (Flow.Subscriber<Object>) result;
            } else if (result instanceof Subscriber) {
                userSubscriber = AdaptersToFlow.subscriber((Subscriber<Object>) result);
            } else {
                userSubscriber = AdaptersToFlow.subscriber(((SubscriberBuilder<Object, Void>) result).build());
            }

            SubscriberWrapper<?, Message<?>> wrapper = new SubscriberWrapper<>(userSubscriber, Message::getPayload,
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

            this.function = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            this.subscriber = wrapper;
        } else {
            Flow.Subscriber<Message<?>> sub;
            if (result instanceof Flow.Subscriber) {
                sub = (Flow.Subscriber<Message<?>>) result;
            } else if (result instanceof Subscriber) {
                sub = AdaptersToFlow.subscriber((Subscriber<Message<?>>) result);
            } else {
                sub = AdaptersToFlow.subscriber(((SubscriberBuilder<Message<?>, Void>) result).build());
            }
            this.function = upstream -> MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            this.subscriber = sub;
        }
    }

    @Override
    public void terminate() {
        Subscriptions.cancel(subscription);
    }
}
