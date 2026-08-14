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
 * A connector for testing applications without an external broker.
 * Substitute the {@code connector} of a specific channel to use {@code smallrye-testing},
 * then send and receive messages programmatically.
 */
@ApplicationScoped
@Connector(TestingConnector.CONNECTOR)
@ConnectorAttribute(name = "run-on-vertx-context", type = "boolean", direction = INCOMING, description = "Whether messages are dispatched on the Vert.x context or not.", defaultValue = "false")
@ConnectorAttribute(name = "broadcast", type = "boolean", direction = INCOMING, description = "Whether the messages are dispatched to multiple consumer", defaultValue = "false")
public class TestingConnector implements InboundConnector, OutboundConnector {

    public static final String CONNECTOR = "smallrye-testing";

    private final Map<String, TestSourceImpl<?>> sources = new HashMap<>();
    private final Map<String, TestSinkImpl<?>> sinks = new HashMap<>();

    @Inject
    ExecutionHolder executionHolder;

    /**
     * Switch the given <em>incoming</em> channels to the test connector.
     *
     * @param channels the channels to switch, must not be {@code null}
     * @return The map of properties that have been set.
     */
    public static Map<String, String> switchIncomingChannelsToTesting(String... channels) {
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
     * Switch the given <em>outgoing</em> channels to the test connector.
     *
     * @param channels the channels to switch, must not be {@code null}
     * @return The map of properties that have been set.
     */
    public static Map<String, String> switchOutgoingChannelsToTesting(String... channels) {
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
     * Clears properties for both {@code smallrye-testing} and {@code smallrye-in-memory} connectors.
     */
    public static void clear() {
        List<String> list = System.getProperties().entrySet().stream()
                .filter(entry -> CONNECTOR.equals(entry.getValue())
                        || InMemoryConnector.CONNECTOR.equals(entry.getValue()))
                .map(entry -> (String) entry.getKey())
                .collect(Collectors.toList());
        list.forEach(System::clearProperty);
    }

    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        TestingConnectorIncomingConfiguration ic = new TestingConnectorIncomingConfiguration(config);
        String name = ic.getChannel();
        boolean broadcast = ic.getBroadcast();
        Vertx vertx = executionHolder.vertx();
        boolean runOnVertxContext = ic.getRunOnVertxContext();
        return sources.computeIfAbsent(name, n -> new TestSourceImpl<>(n, vertx, runOnVertxContext, broadcast)).source;
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        TestingConnectorOutgoingConfiguration ic = new TestingConnectorOutgoingConfiguration(config);
        String name = ic.getChannel();
        return sinks.computeIfAbsent(name, TestSinkImpl::new).sink;
    }

    /**
     * Retrieves the {@link TestIncoming} associated to the given channel, allowing
     * the test to send messages into the application.
     *
     * @param channel the name of the channel, must not be {@code null}
     * @param <T> the type of message or payload sent to the channel
     * @return the incoming channel handle
     * @throws IllegalArgumentException if the channel is not found
     */
    public <T> TestIncoming<T> incoming(String channel) {
        if (channel == null) {
            throw InMemoryExceptions.ex.illegalArgumentChannelMustNotBeNull();
        }
        TestSourceImpl<?> source = sources.get(channel);
        if (source == null) {
            throw InMemoryExceptions.ex.illegalArgumentUnknownChannel(channel);
        }
        //noinspection unchecked
        return (TestIncoming<T>) source;
    }

    /**
     * Retrieves the {@link TestOutgoing} associated to the given channel, allowing
     * the test to verify messages sent by the application.
     *
     * @param channel the name of the channel, must not be {@code null}
     * @param <T> the type of payload received by the channel
     * @return the outgoing channel handle
     * @throws IllegalArgumentException if the channel is not found
     */
    public <T> TestOutgoing<T> outgoing(String channel) {
        if (channel == null) {
            throw InMemoryExceptions.ex.illegalArgumentChannelMustNotBeNull();
        }
        TestSinkImpl<?> sink = sinks.get(channel);
        if (sink == null) {
            throw InMemoryExceptions.ex.illegalArgumentUnknownChannel(channel);
        }
        //noinspection unchecked
        return (TestOutgoing<T>) sink;
    }

    /**
     * @deprecated Use {@link #incoming(String)} instead.
     */
    @Deprecated(forRemoval = true)
    public <T> TestIncoming<T> source(String channel) {
        return incoming(channel);
    }

    /**
     * @deprecated Use {@link #outgoing(String)} instead.
     */
    @Deprecated(forRemoval = true)
    public <T> TestOutgoing<T> sink(String channel) {
        return outgoing(channel);
    }

    static class TestSourceImpl<T> implements InMemorySource<T> {
        final Processor<Message<T>, Message<T>> processor;
        final Flow.Publisher<? extends Message<T>> source;
        private final String name;
        private final Context context;
        private boolean runOnVertxContext;

        TestSourceImpl(String name, Vertx vertx, boolean runOnVertxContext, boolean broadcast) {
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
        public InMemorySource<T> deliver(T messageOrPayload) {
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
        public InMemorySource<T> send(T messageOrPayload) {
            return deliver(messageOrPayload);
        }

        @Override
        public InMemorySource<T> deliver(T payload, Object... metadata) {
            Message<T> message = Message.of(payload);
            for (Object m : metadata) {
                message = message.addMetadata(m);
            }
            Message<T> contextAware = ContextAwareMessage.withContextMetadata(message);
            if (runOnVertxContext) {
                context.runOnContext(() -> processor.onNext(contextAware));
            } else {
                processor.onNext(contextAware);
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

    static class TestSinkImpl<T> implements InMemorySink<T> {
        final Flow.Subscriber<? extends Message<T>> sink;
        private final List<Message<T>> list = new CopyOnWriteArrayList<>();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final AtomicBoolean completed = new AtomicBoolean();
        private final String name;

        TestSinkImpl(String name) {
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
        public List<? extends Message<T>> sent() {
            return new ArrayList<>(list);
        }

        @Override
        public List<? extends Message<T>> received() {
            return sent();
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
