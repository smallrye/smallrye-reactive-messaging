package io.smallrye.reactive.messaging.providers.impl;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderLogging.log;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.ChannelBinding;
import io.smallrye.reactive.messaging.ChannelFactory;
import io.smallrye.reactive.messaging.connector.OutgoingConnectorConfiguration;
import io.smallrye.reactive.messaging.providers.AbstractMediator;
import io.smallrye.reactive.messaging.providers.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.providers.locals.LocalContextMetadata;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

class ChannelBindingImpl<I, O> implements ChannelBinding<I, O> {

    private final Multi<? extends Message<?>> upstream;
    private final ChannelFactory channelFactory;
    // The Reactive Messaging worker pools, used by blocking() and blocking(concurrency).
    private final WorkerPoolRegistry workerPoolRegistry;
    private final boolean blocking;
    // The executor of blocking(Executor), null for the Reactive Messaging worker pool.
    private final Executor executor;
    private final int concurrency;
    private final BiFunction<I, Metadata, Uni<O>> processor;

    ChannelBindingImpl(Multi<? extends Message<?>> upstream, ChannelFactory channelFactory,
            WorkerPoolRegistry workerPoolRegistry) {
        this(upstream, channelFactory, workerPoolRegistry, false, null, 1, null);
    }

    private ChannelBindingImpl(Multi<? extends Message<?>> upstream, ChannelFactory channelFactory,
            WorkerPoolRegistry workerPoolRegistry, boolean blocking, Executor executor, int concurrency,
            BiFunction<I, Metadata, Uni<O>> processor) {
        this.upstream = upstream;
        this.channelFactory = channelFactory;
        this.workerPoolRegistry = workerPoolRegistry;
        this.blocking = blocking;
        this.executor = executor;
        this.concurrency = concurrency;
        this.processor = processor;
    }

    @SuppressWarnings("unchecked")
    private Multi<? extends Message<?>> applyPipeline() {
        if (processor == null && !blocking) {
            return upstream;
        }
        // A blocking binding without steps still moves each message to the worker and back, like the steps would.
        BiFunction<I, Metadata, Uni<O>> steps = processor != null ? processor
                : (payload, metadata) -> Uni.createFrom().item((O) payload);
        Function<Message<?>, Multi<? extends Message<?>>> processMessage = msg -> processMessage(msg, steps);
        return upstream.plug(m -> {
            if (concurrency > 1) {
                return m.onItem().transformToMulti(processMessage).merge(concurrency);
            } else {
                return m.onItem().transformToMulti(processMessage).concatenate();
            }
        });
    }

    @SuppressWarnings("unchecked")
    private Multi<? extends Message<?>> processMessage(Message<?> msg, BiFunction<I, Metadata, Uni<O>> steps) {
        I payload = (I) msg.getPayload();
        // Deferred so the steps run when the inner stream is subscribed, not while the upstream emits.
        // Exceptions thrown by the steps become failures and nack the message.
        Uni<O> processing;
        if (blocking) {
            processing = runBlocking(msg, AbstractMediator.skipContextPropagation(() -> steps.apply(payload,
                    msg.getMetadata())));
        } else {
            processing = Uni.createFrom().deferred(() -> steps.apply(payload, msg.getMetadata()));
        }
        return Multi.createFrom().uni(processing.onItem().transform(msg::withPayload))
                .onFailure().recoverWithMulti(e -> Multi.createFrom()
                        .completionStage(msg.nack(e))
                        .onItem().transformToMultiAndConcatenate(v -> Multi.createFrom().empty()));
    }

    /**
     * Runs the steps of a message on a worker thread, as {@code @Blocking} does: the steps see the message's Vert.x
     * context as the current context, and the result is delivered back on that context, so the sink and the
     * acknowledgement run there too.
     * Each message is processed on its own, so merge(concurrency) runs up to {@code concurrency} at once, and
     * concatenate() keeps them sequential and ordered.
     */
    private Uni<O> runBlocking(Message<?> msg, Uni<O> work) {
        // Captured here, on the thread delivering the message, like the current context of a @Blocking method.
        Context msgContext = msg.getMetadata().get(LocalContextMetadata.class)
                .map(m -> Context.newInstance(m.context()))
                .orElseGet(Vertx::currentContext);
        // Deferred so a rejection (no Vert.x instance, or the worker pools being shut down) nacks the message.
        return Uni.createFrom().deferred(() -> {
            if (executor != null) {
                return workerPoolRegistry.executeWork(msgContext, work, executor);
            }
            // Ordered only when sequential: with concurrency, messages sharing a context must not wait for each other.
            return workerPoolRegistry.executeWork(msgContext, work, null, concurrency == 1);
        });
    }

    /**
     * Returns a new binding applying the given step to the output of the current steps (or to the payload, if there is
     * no step yet). A failure in any step nacks the message.
     */
    @SuppressWarnings("unchecked")
    private <T> ChannelBindingImpl<I, T> then(BiFunction<O, Metadata, Uni<T>> step) {
        BiFunction<I, Metadata, Uni<T>> composed;
        if (processor == null) {
            // No step yet: the output is the payload itself (O is I).
            composed = (payload, metadata) -> step.apply((O) payload, metadata);
        } else {
            composed = (payload, metadata) -> processor.apply(payload, metadata)
                    .onItem().transformToUni(result -> step.apply(result, metadata));
        }
        return new ChannelBindingImpl<>(upstream, channelFactory, workerPoolRegistry, blocking, executor, concurrency,
                composed);
    }

    private ChannelBindingImpl<I, Void> processAndConsume(BiFunction<O, Metadata, Uni<Void>> consumer) {
        return then(consumer);
    }

    @Override
    public ChannelBinding<I, O> blocking() {
        return blocking(1);
    }

    @Override
    public ChannelBinding<I, O> blocking(int concurrency) {
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be >= 1, was " + concurrency);
        }
        return new ChannelBindingImpl<>(upstream, channelFactory, workerPoolRegistry, true, null, concurrency,
                processor);
    }

    @Override
    public ChannelBinding<I, O> blocking(Executor executor) {
        return blocking(executor, 1);
    }

    @Override
    public ChannelBinding<I, O> blocking(Executor executor, int concurrency) {
        Objects.requireNonNull(executor, "executor");
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be >= 1, was " + concurrency);
        }
        return new ChannelBindingImpl<>(upstream, channelFactory, workerPoolRegistry, true, executor, concurrency,
                processor);
    }

    @Override
    public <T> ChannelBinding<I, T> process(Function<O, T> step) {
        return then((output, metadata) -> Uni.createFrom().item(() -> step.apply(output)));
    }

    @Override
    public <T> ChannelBinding<I, T> process(BiFunction<O, Metadata, T> step) {
        return then((output, metadata) -> Uni.createFrom().item(() -> step.apply(output, metadata)));
    }

    @Override
    public <T> ChannelBinding<I, T> processAsync(Function<O, Uni<T>> step) {
        return then((output, metadata) -> step.apply(output));
    }

    @Override
    public <T> ChannelBinding<I, T> processAsync(BiFunction<O, Metadata, Uni<T>> step) {
        return then(step);
    }

    @Override
    public Cancellable subscribe() {
        return applyPipeline()
                .onItem().call(msg -> Uni.createFrom().completionStage(msg.ack()))
                .subscribe().with(v -> {
                }, log::channelBindingSubscriptionError);
    }

    // Terminal operations

    @Override
    public Cancellable subscribe(Consumer<O> consumer) {
        return processAndConsume((payload, metadata) -> {
            consumer.accept(payload);
            return Uni.createFrom().voidItem();
        }).subscribe();
    }

    @Override
    public Cancellable subscribe(BiConsumer<O, Metadata> consumer) {
        return processAndConsume((payload, metadata) -> {
            consumer.accept(payload, metadata);
            return Uni.createFrom().voidItem();
        }).subscribe();
    }

    @Override
    public Cancellable subscribeAsync(Function<O, Uni<Void>> consumer) {
        return processAndConsume((payload, metadata) -> consumer.apply(payload)).subscribe();
    }

    @Override
    public Cancellable subscribeAsync(BiFunction<O, Metadata, Uni<Void>> consumer) {
        return processAndConsume(consumer).subscribe();
    }

    @Override
    public Cancellable to(OutgoingConnectorConfiguration config) {
        return to(channelFactory.outgoing(config));
    }

    @Override
    @SuppressWarnings({ "unchecked" })
    public Cancellable to(Flow.Subscriber<? extends Message<?>> subscriber) {
        CompletingOnCancelSubscriber s = new CompletingOnCancelSubscriber((Flow.Subscriber<Message<?>>) subscriber);
        Cancellable cancellable = applyPipeline().subscribe()
                .with(s.downstream::onSubscribe, s::onNext, s::onError, s::onComplete);
        return () -> {
            cancellable.cancel();
            // Cancelling the binding must not leave the connector sink open.
            s.onComplete();
        };
    }

    /**
     * Forwards signals to the downstream subscriber, guaranteeing a single terminal signal and serial delivery even
     * when completion comes from a cancellation racing with an in-flight {@code onNext}.
     */
    private static final class CompletingOnCancelSubscriber {

        private final Flow.Subscriber<Message<?>> downstream;
        private boolean terminated;

        CompletingOnCancelSubscriber(Flow.Subscriber<Message<?>> downstream) {
            this.downstream = downstream;
        }

        synchronized void onNext(Message<?> item) {
            if (!terminated) {
                downstream.onNext(item);
            }
        }

        synchronized void onError(Throwable failure) {
            if (!terminated) {
                terminated = true;
                downstream.onError(failure);
            }
        }

        synchronized void onComplete() {
            if (!terminated) {
                terminated = true;
                downstream.onComplete();
            }
        }
    }

}
