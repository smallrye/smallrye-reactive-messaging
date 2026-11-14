package io.smallrye.reactive.messaging.providers.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.ChannelBinding;
import io.smallrye.reactive.messaging.ChannelFactory;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.IncomingConnectorConfiguration;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;
import io.smallrye.reactive.messaging.providers.locals.LocalContextMetadata;
import io.vertx.core.Context;
import io.vertx.core.Vertx;

/**
 * Checks that blocking {@link ChannelBinding}s keep the message's Vert.x context, as {@code @Blocking} methods do:
 * the steps see it as the current context, and the acknowledgement runs on it.
 */
class ChannelBindingContextTest extends WeldTestBaseWithoutTails {

    private static final List<Integer> PAYLOADS = List.of(1, 2, 3);

    private IncomingConnectorConfiguration incomingConfig(String channel) {
        var config = new BuilderConfig(Map.of(
                "connector", ContextEmittingConnector.NAME,
                "channel-name", channel));
        return new IncomingConnectorConfiguration() {
            @Override
            public Config config() {
                return config;
            }

            @Override
            public String getChannel() {
                return channel;
            }
        };
    }

    private ContextEmittingConnector connector() {
        return container.select(ContextEmittingConnector.class, ConnectorLiteral.of(ContextEmittingConnector.NAME)).get();
    }

    @Test
    void blockingKeepsTheMessageContext() {
        addBeanClass(ContextEmittingConnector.class);
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        ContextEmittingConnector connector = connector();
        StepRecorder recorder = new StepRecorder();

        Cancellable cancellable = factory.incoming(incomingConfig("ctx-in"), Integer.class)
                .blocking()
                .subscribe(recorder::record);

        await().untilAsserted(() -> assertThat(connector.ackContexts()).hasSize(PAYLOADS.size()));
        assertThat(recorder.payloads).containsExactlyElementsOf(PAYLOADS);
        assertThat(recorder.threads.values()).allSatisfy(name -> assertThat(name).contains("worker"));
        assertContexts(connector, recorder);
        cancellable.cancel();
    }

    @Test
    void blockingWithConcurrencyKeepsTheMessageContext() {
        addBeanClass(ContextEmittingConnector.class);
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        ContextEmittingConnector connector = connector();
        StepRecorder recorder = new StepRecorder();

        Cancellable cancellable = factory.incoming(incomingConfig("ctx-in"), Integer.class)
                .blocking(3)
                .process(i -> i)
                .subscribe(recorder::record);

        await().untilAsserted(() -> assertThat(connector.ackContexts()).hasSize(PAYLOADS.size()));
        assertThat(recorder.payloads).containsExactlyInAnyOrderElementsOf(PAYLOADS);
        assertThat(recorder.threads.values()).allSatisfy(name -> assertThat(name).contains("worker"));
        assertContexts(connector, recorder);
        cancellable.cancel();
    }

    @Test
    void blockingOnCustomExecutorKeepsTheMessageContext() {
        addBeanClass(ContextEmittingConnector.class);
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        ContextEmittingConnector connector = connector();
        StepRecorder recorder = new StepRecorder();
        ExecutorService executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "custom-executor-thread"));

        try {
            Cancellable cancellable = factory.incoming(incomingConfig("ctx-in"), Integer.class)
                    .blocking(executor)
                    .subscribe(recorder::record);

            await().untilAsserted(() -> assertThat(connector.ackContexts()).hasSize(PAYLOADS.size()));
            assertThat(recorder.payloads).containsExactlyElementsOf(PAYLOADS);
            assertThat(recorder.threads.values()).containsOnly("custom-executor-thread");
            assertContexts(connector, recorder);
            cancellable.cancel();
        } finally {
            executor.shutdown();
        }
    }

    private static void assertContexts(ContextEmittingConnector connector, StepRecorder recorder) {
        for (Integer payload : PAYLOADS) {
            Context messageContext = connector.messageContexts().get(payload);
            assertThat(messageContext).as("context of message %d", payload).isNotNull();
            assertThat(recorder.contexts.get(payload)).as("context seen by the step for message %d", payload)
                    .containsSame(messageContext);
            assertThat(connector.ackContexts().get(payload)).as("context of the ack of message %d", payload)
                    .containsSame(messageContext);
        }
    }

    /**
     * Records, for each payload, the thread and the current Vert.x context seen by the step.
     */
    static class StepRecorder {
        final List<Integer> payloads = new CopyOnWriteArrayList<>();
        final Map<Integer, String> threads = new ConcurrentHashMap<>();
        final Map<Integer, Optional<Context>> contexts = new ConcurrentHashMap<>();

        // Not overloaded, so that the method reference is not ambiguous between Consumer and BiConsumer.
        void record(Integer payload) {
            threads.put(payload, Thread.currentThread().getName());
            contexts.put(payload, Optional.ofNullable(Vertx.currentContext()));
            payloads.add(payload);
        }
    }

    /**
     * Emits each message on a Vert.x event loop with its own duplicated context, as connectors do, and records the
     * current context when the message is acknowledged.
     */
    @ApplicationScoped
    @Connector(ContextEmittingConnector.NAME)
    public static class ContextEmittingConnector implements InboundConnector {

        static final String NAME = "context-emitting";

        @Inject
        ExecutionHolder executionHolder;

        private final Map<Integer, Context> messageContexts = new ConcurrentHashMap<>();
        private final Map<Integer, Optional<Context>> ackContexts = new ConcurrentHashMap<>();

        public Map<Integer, Context> messageContexts() {
            return messageContexts;
        }

        public Map<Integer, Optional<Context>> ackContexts() {
            return ackContexts;
        }

        @Override
        public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
            io.vertx.mutiny.core.Context context = executionHolder.vertx().getOrCreateContext();
            return Multi.createFrom().iterable(PAYLOADS)
                    .onItem().transformToUniAndConcatenate(i -> Uni.createFrom()
                            .<Message<Integer>> emitter(e -> context.runOnContext(() -> e.complete(message(i)))));
        }

        private Message<Integer> message(int payload) {
            Message<Integer> message = ContextAwareMessage.withContextMetadata(Message.of(payload));
            messageContexts.put(payload, message.getMetadata(LocalContextMetadata.class).orElseThrow().context());
            return message.withAck(() -> {
                ackContexts.put(payload, Optional.ofNullable(Vertx.currentContext()));
                return CompletableFuture.completedFuture(null);
            });
        }
    }
}
