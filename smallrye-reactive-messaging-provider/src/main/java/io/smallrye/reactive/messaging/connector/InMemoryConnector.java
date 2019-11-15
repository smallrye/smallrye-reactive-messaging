package io.smallrye.reactive.messaging.connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.reactivex.processors.UnicastProcessor;

/**
 * An implementation of connector used for testing applications without having to use external broker.
 * The idea is to substitute the `connector` of a specific channel to use `smallrye-in-memory`.
 * Then your test can send message and checked the received messages.
 */
@ApplicationScoped
@Connector(InMemoryConnector.CONNECTOR)
public class InMemoryConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

    public static final String CONNECTOR = "smallrye-in-memory";

    private Map<String, InMemorySourceImpl<?>> sources = new HashMap<>();
    private Map<String, InMemorySinkImpl<?>> sinks = new HashMap<>();

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        String name = config.getOptionalValue("channel-name", String.class)
                .orElseThrow(() -> new IllegalArgumentException("Invalid incoming configuration, `channel-name` is not set"));
        return sources.computeIfAbsent(name, InMemorySourceImpl::new).source;
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        String name = config.getOptionalValue("channel-name", String.class)
                .orElseThrow(() -> new IllegalArgumentException("Invalid outgoing configuration, `channel-name` is not set"));
        return sinks.computeIfAbsent(name, InMemorySinkImpl::new).sink;
    }

    public <T> InMemorySource<T> source(String channel) {
        if (channel == null) {
            throw new IllegalArgumentException("`channel` must not be `null`");
        }
        InMemorySourceImpl<?> source = sources.get(channel);
        if (source == null) {
            throw new IllegalArgumentException("Unknown channel " + channel);
        }
        //noinspection unchecked
        return (InMemorySource<T>) source;
    }

    public <T> InMemorySink<T> sink(String channel) {
        if (channel == null) {
            throw new IllegalArgumentException("`channel` must not be `null`");
        }
        InMemorySink<?> sink = sinks.get(channel);
        if (sink == null) {
            throw new IllegalArgumentException("Unknown channel " + channel);
        }
        //noinspection unchecked
        return (InMemorySink<T>) sink;
    }

    private class InMemorySourceImpl<T> implements InMemorySource<T> {
        private final UnicastProcessor<Message<T>> processor;
        private final PublisherBuilder<? extends Message<T>> source;
        private final String name;

        private InMemorySourceImpl(String name) {
            this.name = name;
            this.processor = UnicastProcessor.create();
            this.source = ReactiveStreams.fromPublisher(processor);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public InMemorySource<T> send(T messageOrPayload) {
            if (messageOrPayload instanceof Message) {
                //noinspection unchecked
                processor.onNext((Message<T>) messageOrPayload);
            } else {
                processor.onNext(Message.of(messageOrPayload));
            }
            return this;
        }

        @Override
        public void complete() {
            processor.onComplete();
        }

        @Override
        public void fail(Throwable failure) {
            processor.onError(failure);
        }
    }

    private class InMemorySinkImpl<T> implements InMemorySink<T> {
        private final SubscriberBuilder<? extends Message<T>, Void> sink;
        private final List<Message<T>> list = new CopyOnWriteArrayList<>();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final AtomicBoolean completed = new AtomicBoolean();
        private final String name;

        private InMemorySinkImpl(String name) {
            this.name = name;
            this.sink = ReactiveStreams.<Message<T>> builder()
                    .flatMapCompletionStage(m -> {
                        list.add(m);
                        return m.ack().thenApply(x -> m);
                    })
                    .onError(err -> failure.compareAndSet(null, err))
                    .onComplete(() -> completed.compareAndSet(false, true))
                    .ignore();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public List<? extends Message<T>> received() {
            return new ArrayList<>(list);
        }

        public void clear() {
            completed.set(false);
            failure.set(null);
            list.clear();
        }

        @Override
        public boolean hasCompleted() {
            return completed.get();
        }

        @Override
        public boolean hasFailed() {
            return getFailure() != null;
        }

        @Override
        public Throwable getFailure() {
            return failure.get();
        }
    }
}
