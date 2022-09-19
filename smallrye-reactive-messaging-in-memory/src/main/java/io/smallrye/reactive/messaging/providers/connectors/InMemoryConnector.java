package io.smallrye.reactive.messaging.providers.connectors;

import static io.smallrye.reactive.messaging.providers.connectors.i18n.InMemoryExceptions.ex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Processor;

import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;

/**
 * An implementation of connector used for testing applications without having to use external broker.
 * The idea is to substitute the `connector` of a specific channel to use `smallrye-in-memory`.
 * Then, your test can send message and checked the received messages.
 */
@ApplicationScoped
@Connector(InMemoryConnector.CONNECTOR)
public class InMemoryConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

    public static final String CONNECTOR = "smallrye-in-memory";

    private final Map<String, InMemorySourceImpl<?>> sources = new HashMap<>();
    private final Map<String, InMemorySinkImpl<?>> sinks = new HashMap<>();

    /**
     * Switch the given <em>incoming</em> channel to in-memory. It replaces the previously used connector with the
     * in-memory connector.
     * <p>
     * This method is generally used before tests to avoid using an external broker for a specific channel. You can then
     * retrieve the {@link InMemorySource} using:
     * <code><pre>
     *     &#64;Inject @Any
     *     InMemoryConnector connector;
     *
     *     //...
     *
     *     &#64;Before
     *     public void setup() {
     *         InMemoryConnector.switchIncomingChannelsToInMemory("my-channel");
     *     }
     *
     *     // ..
     *
     *     InMemorySource&lt;Integer&gt; channel = connector.source("my-channel");
     *     channel.send(1);
     *     channel.send(2);
     *
     * </pre></code>
     * <p>
     *
     * @param channels the channels to switch, must not be {@code null}, must not contain {@code null}, must not contain
     *        a blank value
     * @return The map of properties that have been defined. The method sets the system properties, but give
     *         you this map to pass the properties around if needed.
     */
    public static Map<String, String> switchIncomingChannelsToInMemory(String... channels) {
        Map<String, String> properties = new LinkedHashMap<>();
        for (String channel : channels) {
            if (channel == null || channel.trim().isEmpty()) {
                throw ex.illegalArgumentChannelNameNull();
            }
            String key = "mp.messaging.incoming." + channel + ".connector";
            properties.put(key, CONNECTOR);
            System.setProperty(key, CONNECTOR);
        }
        return properties;
    }

    /**
     * Switch the given <em>outgoing</em> channel to in-memory. It replaces the previously used connector with the
     * in-memory connector.
     * <p>
     * This method is generally used before tests to avoid using an external broker for a specific channel. You can then
     * retrieve the {@link InMemorySink} using:
     * <code><pre>
     *     &#64;Inject @Any
     *     InMemoryConnector connector;
     *
     *     //...
     *
     *     &#64;Before
     *     public void setup() {
     *         InMemoryConnector.switchOutgoingChannelsToInMemory("my-channel");
     *     }
     *
     *     // ..
     *
     *     InMemorySink&lt;Integer&gt; channel = connector.sink("my-channel");
     *     assertThat(channel.received()).hasSize(3).extracting(Message::getPayload).containsExactly(1, 2);
     *
     * </pre></code>
     * <p>
     *
     * @param channels the channels to switch, must not be {@code null}, must not contain {@code null}, must not contain
     *        a blank value
     * @return The map of properties that have been defined. The method sets the system properties, but give
     *         you this map to pass these properties around if needed.
     */
    public static Map<String, String> switchOutgoingChannelsToInMemory(String... channels) {
        Map<String, String> properties = new LinkedHashMap<>();
        for (String channel : channels) {
            if (channel == null || channel.trim().isEmpty()) {
                throw ex.illegalArgumentChannelNameNull();
            }
            String key = "mp.messaging.outgoing." + channel + ".connector";
            properties.put(key, CONNECTOR);
            System.setProperty(key, CONNECTOR);
        }
        return properties;
    }

    /**
     * Switch back the channel to their original connector.
     * <p>
     * This method is generally used after tests to reset the original configuration.
     */
    public static void clear() {
        List<String> list = System.getProperties().entrySet().stream()
                .filter(entry -> CONNECTOR.equals(entry.getValue()))
                .map(entry -> (String) entry.getKey())
                .collect(Collectors.toList());
        list.forEach(System::clearProperty);
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        String name = config.getOptionalValue("channel-name", String.class)
                .orElseThrow(ex::illegalArgumentInvalidIncomingConfig);

        boolean broadcast = config.getOptionalValue("broadcast", Boolean.class)
                .orElse(false);
        return sources.computeIfAbsent(name, n -> new InMemorySourceImpl<>(n, broadcast)).source;
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        String name = config.getOptionalValue("channel-name", String.class)
                .orElseThrow(ex::illegalArgumentInvalidOutgoingConfig);
        return sinks.computeIfAbsent(name, InMemorySinkImpl::new).sink;
    }

    /**
     * Retrieves an {@link InMemorySource} associated to the channel named {@code channel}.
     * This channel must use the in-memory connected.
     * <p>
     * The returned {@link InMemorySource} lets you send messages or payloads to the channel, mocking the real
     * interactions.
     *
     * @param channel the name of the channel, must not be {@code null}
     * @param <T> the type of message or payload sent to the channel
     * @return the source
     * @throws IllegalArgumentException if the channel name is {@code null} or if the channel is not associated with the
     *         in-memory connector.
     * @see #switchIncomingChannelsToInMemory(String...)
     */
    public <T> InMemorySource<T> source(String channel) {
        if (channel == null) {
            throw ex.illegalArgumentChannelMustNotBeNull();
        }
        InMemorySourceImpl<?> source = sources.get(channel);
        if (source == null) {
            throw ex.illegalArgumentUnknownChannel(channel);
        }
        //noinspection unchecked
        return (InMemorySource<T>) source;
    }

    /**
     * Retrieves an {@link InMemorySink} associated to the channel named {@code channel}.
     * This channel must use the in-memory connected.
     * <p>
     * The returned {@link InMemorySink} lets you checks the messages sent to the channel.
     *
     * @param channel the name of the channel, must not be {@code null}
     * @param <T> the type of payload received by the channel
     * @return the sink
     * @throws IllegalArgumentException if the channel name is {@code null} or if the channel is not associated with the
     *         in-memory connector.
     * @see #switchOutgoingChannelsToInMemory(String...)
     */
    public <T> InMemorySink<T> sink(String channel) {
        if (channel == null) {
            throw ex.illegalArgumentChannelMustNotBeNull();
        }
        InMemorySink<?> sink = sinks.get(channel);
        if (sink == null) {
            throw ex.illegalArgumentUnknownChannel(channel);
        }
        //noinspection unchecked
        return (InMemorySink<T>) sink;
    }

    private static class InMemorySourceImpl<T> implements InMemorySource<T> {
        private final Processor<Message<T>, Message<T>> processor;
        private final PublisherBuilder<? extends Message<T>> source;
        private final String name;

        private InMemorySourceImpl(String name, boolean broadcast) {
            this.name = name;
            if (broadcast) {
                processor = BroadcastProcessor.create();
            } else {
                processor = UnicastProcessor.create();
            }
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

    private static class InMemorySinkImpl<T> implements InMemorySink<T> {
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

        @Override
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
