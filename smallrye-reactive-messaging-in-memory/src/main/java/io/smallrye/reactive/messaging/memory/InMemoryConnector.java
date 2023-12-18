package io.smallrye.reactive.messaging.memory;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.memory.i18n.InMemoryExceptions;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;
import io.smallrye.reactive.messaging.providers.locals.LocalContextMetadata;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

/**
 * An implementation of connector used for testing applications without having to use external broker.
 * The idea is to substitute the `connector` of a specific channel to use `smallrye-in-memory`.
 * Then, your test can send message and checked the received messages.
 */
@ApplicationScoped
@Connector(InMemoryConnector.CONNECTOR)
@ConnectorAttribute(name = "run-on-vertx-context", type = "boolean", direction = INCOMING, description = "Whether messages are dispatched on the Vert.x context or not.", defaultValue = "false")
@ConnectorAttribute(name = "broadcast", type = "boolean", direction = INCOMING, description = "Whether the messages are dispatched to multiple consumer", defaultValue = "false")
public class InMemoryConnector implements InboundConnector, OutboundConnector {

    public static final String CONNECTOR = "smallrye-in-memory";

    private final Map<String, InMemorySourceImpl<?>> sources = new HashMap<>();
    private final Map<String, InMemorySinkImpl<?>> sinks = new HashMap<>();

    @Inject
    ExecutionHolder executionHolder;

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
                throw InMemoryExceptions.ex.illegalArgumentChannelNameNull();
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
                throw InMemoryExceptions.ex.illegalArgumentChannelNameNull();
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
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        InMemoryConnectorIncomingConfiguration ic = new InMemoryConnectorIncomingConfiguration(config);
        String name = ic.getChannel();
        boolean broadcast = ic.getBroadcast();
        Vertx vertx = executionHolder.vertx();
        boolean runOnVertxContext = ic.getRunOnVertxContext();
        return sources.computeIfAbsent(name, n -> new InMemorySourceImpl<>(n, vertx, runOnVertxContext, broadcast)).source;
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        InMemoryConnectorOutgoingConfiguration ic = new InMemoryConnectorOutgoingConfiguration(config);
        String name = ic.getChannel();
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
            throw InMemoryExceptions.ex.illegalArgumentChannelMustNotBeNull();
        }
        InMemorySourceImpl<?> source = sources.get(channel);
        if (source == null) {
            throw InMemoryExceptions.ex.illegalArgumentUnknownChannel(channel);
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
            throw InMemoryExceptions.ex.illegalArgumentChannelMustNotBeNull();
        }
        InMemorySink<?> sink = sinks.get(channel);
        if (sink == null) {
            throw InMemoryExceptions.ex.illegalArgumentUnknownChannel(channel);
        }
        //noinspection unchecked
        return (InMemorySink<T>) sink;
    }

    private static class InMemorySourceImpl<T> implements InMemorySource<T> {
        private final Processor<Message<T>, Message<T>> processor;
        private final Flow.Publisher<? extends Message<T>> source;
        private final String name;
        private final Context context;
        private boolean runOnVertxContext;

        private InMemorySourceImpl(String name, Vertx vertx, boolean runOnVertxContext, boolean broadcast) {
            this.name = name;
            this.context = vertx.getOrCreateContext();
            this.runOnVertxContext = runOnVertxContext;
            if (broadcast) {
                processor = BroadcastProcessor.create();
            } else {
                processor = UnicastProcessor.create();
            }
            this.source = Multi.createFrom().publisher(processor);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public InMemorySource<T> send(T messageOrPayload) {
            if (messageOrPayload instanceof Message) {
                //noinspection unchecked
                if (runOnVertxContext) {
                    context.runOnContext(
                            () -> processor.onNext(ContextAwareMessage.withContextMetadata((Message<T>) messageOrPayload)));
                } else {
                    processor.onNext(ContextAwareMessage.withContextMetadata((Message<T>) messageOrPayload));
                }
            } else {
                if (runOnVertxContext) {
                    context.runOnContext(() -> processor.onNext(ContextAwareMessage.of(messageOrPayload)));
                } else {
                    processor.onNext(ContextAwareMessage.of(messageOrPayload));
                }
            }
            return this;
        }

        @Override
        public InMemorySource<T> runOnVertxContext(boolean runOnVertxContext) {
            this.runOnVertxContext = runOnVertxContext;
            return this;
        }

        @Override
        public void complete() {
            if (runOnVertxContext) {
                context.runOnContext(() -> processor.onComplete());
            } else {
                processor.onComplete();
            }
        }

        @Override
        public void fail(Throwable failure) {
            if (runOnVertxContext) {
                context.runOnContext(() -> processor.onError(failure));
            } else {
                processor.onError(failure);
            }
        }
    }

    private static class InMemorySinkImpl<T> implements InMemorySink<T> {
        private final Flow.Subscriber<? extends Message<T>> sink;
        private final List<Message<T>> list = new CopyOnWriteArrayList<>();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final AtomicBoolean completed = new AtomicBoolean();
        private final String name;

        private InMemorySinkImpl(String name) {
            this.name = name;
            this.sink = MultiUtils.via(multi -> multi.call(m -> {
                list.add(m);
                Uni<Void> ack = Uni.createFrom().completionStage(m::ack);
                if (m.getMetadata(LocalContextMetadata.class).isPresent()) {
                    Context ctx = Context.newInstance(m.getMetadata(LocalContextMetadata.class).get().context());
                    ack = ack.emitOn(ctx::runOnContext);
                }
                return ack;
            }).onFailure().invoke(err -> failure.compareAndSet(null, err))
                    .onCompletion().invoke(() -> completed.compareAndSet(false, true)));
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
