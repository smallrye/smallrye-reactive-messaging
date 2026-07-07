package io.smallrye.reactive.messaging.providers.impl;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderLogging.log;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.ChannelBinding;
import io.smallrye.reactive.messaging.ChannelFactory;

class ChannelBindingImpl<I, O> implements ChannelBinding<I, O> {

    private final Multi<? extends Message<?>> upstream;
    private final ChannelFactory channelFactory;
    private final Executor executor;
    private final int concurrency;
    private final BiFunction<I, Metadata, Uni<O>> processor;

    ChannelBindingImpl(Multi<? extends Message<?>> upstream, ChannelFactory channelFactory) {
        this(upstream, channelFactory, null, 1, null);
    }

    private ChannelBindingImpl(Multi<? extends Message<?>> upstream, ChannelFactory channelFactory,
            Executor executor, int concurrency, BiFunction<I, Metadata, Uni<O>> processor) {
        this.upstream = upstream;
        this.channelFactory = channelFactory;
        this.executor = executor;
        this.concurrency = concurrency;
        this.processor = processor;
    }

    private Multi<? extends Message<?>> applyPipeline() {
        Multi<? extends Message<?>> multi = upstream.plug(m -> executor != null ? m.emitOn(executor) : m);
        if (processor != null) {
            Function<Message<?>, Multi<? extends Message<?>>> processMessage = this::processMessage;
            multi = multi.plug(m -> {
                if (concurrency > 1) {
                    return m.onItem().transformToMulti(processMessage).merge(concurrency);
                } else {
                    return m.onItem().transformToMulti(processMessage).concatenate();
                }
            });
        }
        return multi;
    }

    @SuppressWarnings("unchecked")
    private Multi<? extends Message<?>> processMessage(Message<?> msg) {
        try {
            I payload = (I) msg.getPayload();
            return Multi.createFrom().uni(processor.apply(payload, msg.getMetadata())
                    .onItem().transform(msg::withPayload))
                    .onFailure().recoverWithMulti(e -> Multi.createFrom()
                            .completionStage(msg.nack(e))
                            .onItem().transformToMultiAndConcatenate(v -> Multi.createFrom().empty()));
        } catch (Exception e) {
            return Multi.createFrom().completionStage(msg.nack(e))
                    .onItem().transformToMultiAndConcatenate(v -> Multi.createFrom().empty());
        }
    }

    @SuppressWarnings("unchecked")
    private ChannelBindingImpl<I, Void> processAndConsume(BiFunction<O, Metadata, Uni<Void>> consumer) {
        BiFunction<I, Metadata, Uni<Void>> composed;
        if (processor == null) {
            composed = (BiFunction<I, Metadata, Uni<Void>>) consumer;
        } else {
            composed = (payload, metadata) -> processor.apply(payload, metadata)
                    .onItem().transformToUni(result -> consumer.apply(result, metadata));
        }
        return new ChannelBindingImpl<>(upstream, channelFactory, executor, concurrency, composed);
    }

    @Override
    public ChannelBinding<I, O> blocking() {
        return blocking(Infrastructure.getDefaultExecutor(), 1);
    }

    @Override
    public ChannelBinding<I, O> blocking(int concurrency) {
        return blocking(Infrastructure.getDefaultExecutor(), concurrency);
    }

    @Override
    public ChannelBinding<I, O> blocking(Executor executor) {
        return blocking(executor, 1);
    }

    @Override
    public ChannelBinding<I, O> blocking(Executor executor, int concurrency) {
        return new ChannelBindingImpl<>(upstream, channelFactory, executor, concurrency, processor);
    }

    @Override
    public <T> ChannelBinding<I, T> process(Function<I, T> proc) {
        return new ChannelBindingImpl<>(upstream, channelFactory, executor, concurrency,
                (payload, metadata) -> Uni.createFrom().item(proc.apply(payload)));
    }

    @Override
    public <T> ChannelBinding<I, T> process(BiFunction<I, Metadata, T> processor) {
        return new ChannelBindingImpl<>(upstream, channelFactory, executor, concurrency,
                (payload, meta) -> Uni.createFrom().item(processor.apply(payload, meta)));
    }

    @Override
    public <T> ChannelBinding<I, T> processAsync(Function<I, Uni<T>> processor) {
        return new ChannelBindingImpl<>(upstream, channelFactory, executor, concurrency,
                (payload, metadata) -> processor.apply(payload));
    }

    @Override
    public <T> ChannelBinding<I, T> processAsync(BiFunction<I, Metadata, Uni<T>> processor) {
        return new ChannelBindingImpl<>(upstream, channelFactory, executor, concurrency, processor);
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
    public Cancellable subscribe(Function<O, Uni<Void>> consumer) {
        return processAndConsume((payload, metadata) -> consumer.apply(payload)).subscribe();
    }

    @Override
    public Cancellable subscribe(BiFunction<O, Metadata, Uni<Void>> consumer) {
        return processAndConsume(consumer).subscribe();
    }

    @Override
    public Cancellable to(String channel, Config config) {
        return to(channelFactory.outgoing(channel, config));
    }

    @Override
    public Cancellable to(String channel, Map<String, String> channelConfig) {
        return to(channelFactory.outgoing(channel, channelConfig));
    }

    @Override
    @SuppressWarnings({ "unchecked" })
    public Cancellable to(Flow.Subscriber<? extends Message<?>> subscriber) {
        Flow.Subscriber<Message<?>> s = (Flow.Subscriber<Message<?>>) subscriber;
        return applyPipeline().subscribe().with(s::onSubscribe, s::onNext, s::onError, s::onComplete);
    }

}
