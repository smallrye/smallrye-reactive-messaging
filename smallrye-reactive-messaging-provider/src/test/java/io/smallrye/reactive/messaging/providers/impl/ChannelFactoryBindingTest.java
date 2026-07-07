package io.smallrye.reactive.messaging.providers.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.function.Consumer;

import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.ChannelFactory;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.connector.IncomingConnectorConfiguration;
import io.smallrye.reactive.messaging.connector.MapBasedConfig;
import io.smallrye.reactive.messaging.connector.OutgoingConnectorConfiguration;
import io.smallrye.reactive.messaging.providers.connectors.MyDummyConnector;

class ChannelFactoryBindingTest extends WeldTestBaseWithoutTails {

    private IncomingConnectorConfiguration incomingConfig(String channel) {
        return incomingConfig(channel, Map.of());
    }

    private IncomingConnectorConfiguration incomingConfig(String channel, Map<String, String> extra) {
        var values = new java.util.HashMap<>(Map.of(
                "connector", "dummy",
                "channel-name", channel));
        values.putAll(extra);
        var config = new MapBasedConfig(values);
        return new IncomingConnectorConfiguration() {
            @Override
            public org.eclipse.microprofile.config.Config config() {
                return config;
            }

            @Override
            public String getChannel() {
                return channel;
            }
        };
    }

    private OutgoingConnectorConfiguration outgoingConfig(String channel) {
        var config = new MapBasedConfig(Map.of(
                "connector", "dummy",
                "channel-name", channel));
        return new OutgoingConnectorConfiguration() {
            @Override
            public org.eclipse.microprofile.config.Config config() {
                return config;
            }

            @Override
            public String getChannel() {
                return channel;
            }
        };
    }

    @Test
    void incomingCreatesChannelAndRegistersConnectorName() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        ChannelRegistry registry = get(ChannelRegistry.class);

        List<Integer> received = new CopyOnWriteArrayList<>();
        factory.incoming(incomingConfig("test-in"), Integer.class)
                .subscribe(payload -> {
                    received.add(payload);
                });

        await().untilAsserted(() -> assertThat(received).containsExactly(2, 3, 4));
        assertThat(registry.getPublishers("test-in")).isNotEmpty();
        assertThat(registry.getIncomingConnectorName("test-in")).isEqualTo("smallrye-dummy");
    }

    @Test
    void outgoingWithEmitterCreatesChannelAndRegistersConnectorName() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        ChannelRegistry registry = get(ChannelRegistry.class);

        MutinyEmitter<String> emitter = factory.outgoing(outgoingConfig("test-out"), MutinyEmitter.class);

        assertThat(emitter).isNotNull();
        assertThat(registry.getSubscribers("test-out")).isNotEmpty();
        assertThat(registry.getOutgoingConnectorName("test-out")).isEqualTo("smallrye-dummy");
    }

    @Test
    @SuppressWarnings("unchecked")
    void outgoingWithEmitterSendsMessages() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Emitter<String> emitter = factory.outgoing(outgoingConfig("test-out"), Emitter.class);

        emitter.send("hello");
        emitter.send("world");

        await().untilAsserted(() -> assertThat(connector.list()).containsExactly("hello", "world"));
    }

    @Test
    void outgoingWithSourcePublisherRegistersConnectorName() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        ChannelRegistry registry = get(ChannelRegistry.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Flow.Publisher<Message<?>> source = Multi.createFrom().items("a", "b", "c")
                .map(Message::of);

        factory.outgoing(outgoingConfig("test-out-src"), source);

        assertThat(registry.getOutgoingConnectorName("test-out-src")).isEqualTo("smallrye-dummy");
        await().untilAsserted(() -> assertThat(connector.list()).containsExactly("a", "b", "c"));
    }

    @Test
    void bindWithPayloadConsumer() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        List<Integer> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming(incomingConfig("test-in"), Integer.class)
                .subscribe(payload -> {
                    received.add(payload);
                });

        await().untilAsserted(() -> assertThat(received).containsExactly(2, 3, 4));
        cancellable.cancel();
    }

    @Test
    void bindWithPayloadAndMetadataConsumer() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        List<Integer> received = new CopyOnWriteArrayList<>();
        List<Metadata> metadataList = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming(incomingConfig("test-in"), Integer.class)
                .subscribe((payload, metadata) -> {
                    received.add(payload);
                    metadataList.add(metadata);
                });

        await().untilAsserted(() -> {
            assertThat(received).containsExactly(2, 3, 4);
            assertThat(metadataList).hasSize(3);
        });
        cancellable.cancel();
    }

    @Test
    void bindWithBlockingConsumer() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        List<Integer> received = new CopyOnWriteArrayList<>();
        List<String> threadNames = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming(incomingConfig("test-in"), Integer.class)
                .blocking()
                .subscribe(payload -> {
                    received.add(payload);
                    threadNames.add(Thread.currentThread().getName());
                });

        await().untilAsserted(() -> assertThat(received).containsExactly(2, 3, 4));
        assertThat(threadNames).allSatisfy(name -> assertThat(name).doesNotContain("main"));
        cancellable.cancel();
    }

    @Test
    void bindWithAsyncConsumer() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        List<Integer> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming(incomingConfig("test-in"), Integer.class)
                .subscribe(payload -> {
                    received.add(payload);
                    return Uni.createFrom().voidItem();
                });

        await().untilAsserted(() -> assertThat(received).containsExactly(2, 3, 4));
        cancellable.cancel();
    }

    @Test
    void bindWithBlockingConcurrency() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        List<Integer> received = new CopyOnWriteArrayList<>();
        List<String> threadNames = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming(incomingConfig("test-in"), Integer.class)
                .blocking(3)
                .subscribe(payload -> {
                    received.add(payload);
                    threadNames.add(Thread.currentThread().getName());
                });

        await().untilAsserted(() -> assertThat(received).containsExactlyInAnyOrder(2, 3, 4));
        assertThat(threadNames).allSatisfy(name -> assertThat(name).doesNotContain("main"));
        cancellable.cancel();
    }

    @Test
    void bindProcessAndForwardToOutgoing() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Flow.Subscriber<? extends Message<?>> subscriber = factory.outgoing(outgoingConfig("test-out"));

        factory.incoming(incomingConfig("test-in"), Integer.class)
                .process(i -> "value-" + i)
                .to(subscriber);

        await().untilAsserted(() -> assertThat(connector.list()).containsExactly("value-2", "value-3", "value-4"));
    }

    @Test
    void bindForwardToOutgoingWithoutProcess() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Flow.Subscriber<? extends Message<?>> subscriber = factory.outgoing(outgoingConfig("test-out"));

        factory.incoming(incomingConfig("test-in"), Integer.class)
                .to(subscriber);

        await().untilAsserted(() -> assertThat(connector.list()).containsExactly("2", "3", "4"));
    }

    @Test
    void bindProcessAndForwardToOutgoingConfig() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        factory.incoming(incomingConfig("test-in"), Integer.class)
                .process(i -> "value-" + i)
                .to(outgoingConfig("test-out"));

        await().untilAsserted(() -> assertThat(connector.list()).containsExactly("value-2", "value-3", "value-4"));
    }

    @Test
    void bindBlockingProcessAndForwardToOutgoing() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Flow.Subscriber<? extends Message<?>> subscriber = factory.outgoing(outgoingConfig("test-out"));

        List<String> threadNames = new CopyOnWriteArrayList<>();
        factory.incoming(incomingConfig("test-in"), Integer.class)
                .blocking()
                .process(i -> {
                    threadNames.add(Thread.currentThread().getName());
                    return "blocking-" + i;
                })
                .to(subscriber);

        await().untilAsserted(() -> assertThat(connector.list()).containsExactly("blocking-2", "blocking-3", "blocking-4"));
        assertThat(threadNames).allSatisfy(name -> assertThat(name).doesNotContain("main"));
    }

    @Test
    void bindWithBlockingCustomExecutor() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        ExecutorService executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "custom-executor-thread"));

        List<Integer> received = new CopyOnWriteArrayList<>();
        List<String> threadNames = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming(incomingConfig("test-in"), Integer.class)
                .blocking(executor)
                .subscribe(payload -> {
                    received.add(payload);
                    threadNames.add(Thread.currentThread().getName());
                });

        await().untilAsserted(() -> assertThat(received).containsExactly(2, 3, 4));
        assertThat(threadNames).allSatisfy(name -> assertThat(name).isEqualTo("custom-executor-thread"));
        cancellable.cancel();
        executor.shutdown();
    }

    @Test
    void bindWithBlockingCustomExecutorAndConcurrency() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        ExecutorService executor = Executors.newFixedThreadPool(3, r -> {
            Thread t = new Thread(r, "pool-thread");
            t.setDaemon(true);
            return t;
        });

        List<Integer> received = new CopyOnWriteArrayList<>();
        List<String> threadNames = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming(incomingConfig("test-in"), Integer.class)
                .blocking(executor, 3)
                .subscribe(payload -> {
                    received.add(payload);
                    threadNames.add(Thread.currentThread().getName());
                });

        await().untilAsserted(() -> assertThat(received).containsExactlyInAnyOrder(2, 3, 4));
        assertThat(threadNames).allSatisfy(name -> assertThat(name).isEqualTo("pool-thread"));
        cancellable.cancel();
        executor.shutdown();
    }

    @Test
    void bindProcessWithConcurrencyToOutgoing() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Flow.Subscriber<? extends Message<?>> subscriber = factory.outgoing(outgoingConfig("test-out"));

        factory.incoming(incomingConfig("test-in"), Integer.class)
                .blocking(3)
                .process(i -> "concurrent-" + i)
                .to(subscriber);

        await().untilAsserted(() -> assertThat(connector.list())
                .containsExactlyInAnyOrder("concurrent-2", "concurrent-3", "concurrent-4"));
    }

    @Test
    void bindProcessAndSubscribe() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        List<String> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming(incomingConfig("test-in"), Integer.class)
                .process(i -> "processed-" + i)
                .subscribe(payload -> {
                    received.add(payload);
                });

        await().untilAsserted(() -> assertThat(received).containsExactly("processed-2", "processed-3", "processed-4"));
        cancellable.cancel();
    }

    @Test
    void subscribeNoArgAcksMessages() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        List<Integer> received = new CopyOnWriteArrayList<>();
        factory.incoming(incomingConfig("test-in"), Integer.class)
                .process(i -> {
                    received.add(i);
                    return i;
                })
                .subscribe();

        await().untilAsserted(() -> assertThat(received).containsExactly(2, 3, 4));
    }

    @Test
    void subscribeNoArgWithoutProcessAcksMessages() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        var emitter = new java.util.concurrent.atomic.AtomicReference<io.smallrye.mutiny.subscription.MultiEmitter<? super Message<?>>>();
        Multi<Message<?>> source = Multi.createFrom().emitter(e -> emitter.set(e));

        List<Object> acked = new CopyOnWriteArrayList<>();
        Multi<Message<?>> tracked = source
                .map(msg -> msg.withAckWithMetadata(metadata -> {
                    acked.add(msg.getPayload());
                    return msg.ack();
                }));

        factory.bind(tracked).subscribe();

        await().untilAsserted(() -> assertThat(emitter.get()).isNotNull());
        emitter.get().emit(Message.of(1));
        emitter.get().emit(Message.of(2));
        emitter.get().emit(Message.of(3));

        await().untilAsserted(() -> assertThat(acked).containsExactly(1, 2, 3));
    }

    @Test
    void subscribeConsumerNacksOnFailure() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        var emitter = new java.util.concurrent.atomic.AtomicReference<io.smallrye.mutiny.subscription.MultiEmitter<? super Message<?>>>();
        Multi<Message<?>> source = Multi.createFrom().emitter(e -> emitter.set(e));

        List<Object> nacked = new CopyOnWriteArrayList<>();
        Multi<Message<?>> tracked = source
                .map(msg -> msg.withNackWithMetadata((e, metadata) -> {
                    nacked.add(msg.getPayload());
                    return msg.nack(e);
                }));

        factory.bind(tracked, Integer.class)
                .subscribe(payload -> {
                    if (payload == 3) {
                        throw new RuntimeException("fail on 3");
                    }
                });

        await().untilAsserted(() -> assertThat(emitter.get()).isNotNull());
        emitter.get().emit(Message.of(2));
        emitter.get().emit(Message.of(3));
        emitter.get().emit(Message.of(4));

        await().untilAsserted(() -> assertThat(nacked).contains(3));
    }

    @Test
    void processFailureNacksAndContinues() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        var emitter = new java.util.concurrent.atomic.AtomicReference<io.smallrye.mutiny.subscription.MultiEmitter<? super Message<?>>>();
        Multi<Message<?>> source = Multi.createFrom().emitter(e -> emitter.set(e));

        List<Object> nacked = new CopyOnWriteArrayList<>();
        Multi<Message<?>> tracked = source
                .map(msg -> msg.withNackWithMetadata((e, metadata) -> {
                    nacked.add(msg.getPayload());
                    return msg.nack(e);
                }));

        Flow.Subscriber<? extends Message<?>> subscriber = factory.outgoing(outgoingConfig("test-out"));

        factory.bind(tracked, Integer.class)
                .process(i -> {
                    if (i == 3) {
                        throw new RuntimeException("fail on 3");
                    }
                    return "ok-" + i;
                })
                .to(subscriber);

        await().untilAsserted(() -> assertThat(emitter.get()).isNotNull());
        emitter.get().emit(Message.of(2));
        emitter.get().emit(Message.of(3));
        emitter.get().emit(Message.of(4));

        await().untilAsserted(() -> {
            assertThat(connector.list()).containsExactly("ok-2", "ok-4");
            assertThat(nacked).contains(3);
        });
    }

    @Test
    void processAsyncFailureNacks() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        var emitter = new java.util.concurrent.atomic.AtomicReference<io.smallrye.mutiny.subscription.MultiEmitter<? super Message<?>>>();
        Multi<Message<?>> source = Multi.createFrom().emitter(e -> emitter.set(e));

        List<Object> nacked = new CopyOnWriteArrayList<>();
        Multi<Message<?>> tracked = source
                .map(msg -> msg.withNackWithMetadata((e, metadata) -> {
                    nacked.add(msg.getPayload());
                    return msg.nack(e);
                }));

        List<String> received = new CopyOnWriteArrayList<>();
        factory.bind(tracked, Integer.class)
                .processAsync((payload, metadata) -> {
                    if (payload == 3) {
                        return Uni.createFrom().failure(new RuntimeException("async fail"));
                    }
                    return Uni.createFrom().item("ok-" + payload);
                })
                .subscribe((Consumer<String>) received::add);

        await().untilAsserted(() -> assertThat(emitter.get()).isNotNull());
        emitter.get().emit(Message.of(2));
        emitter.get().emit(Message.of(3));
        emitter.get().emit(Message.of(4));

        await().untilAsserted(() -> {
            assertThat(received).containsExactly("ok-2", "ok-4");
            assertThat(nacked).contains(3);
        });
    }

    @Test
    void bindWithoutTypeSubscribe() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        var emitter = new java.util.concurrent.atomic.AtomicReference<io.smallrye.mutiny.subscription.MultiEmitter<? super Message<?>>>();
        Multi<Message<?>> source = Multi.createFrom().emitter(e -> emitter.set(e));

        List<Object> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.bind(source)
                .subscribe(payload -> {
                    received.add(payload);
                });

        await().untilAsserted(() -> assertThat(emitter.get()).isNotNull());
        emitter.get().emit(Message.of(1));
        emitter.get().emit(Message.of(2));
        emitter.get().emit(Message.of(3));

        await().untilAsserted(() -> assertThat(received).containsExactly(1, 2, 3));
        cancellable.cancel();
    }

    @Test
    void subscribeCancelStopsDelivery() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        var emitter = new java.util.concurrent.atomic.AtomicReference<io.smallrye.mutiny.subscription.MultiEmitter<? super Message<?>>>();
        Multi<Message<?>> source = Multi.createFrom().emitter(e -> emitter.set(e));

        List<Integer> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.bind(source, Integer.class)
                .subscribe((Consumer<Integer>) received::add);

        await().untilAsserted(() -> assertThat(emitter.get()).isNotNull());

        emitter.get().emit(Message.of(1));
        emitter.get().emit(Message.of(2));
        await().untilAsserted(() -> assertThat(received).containsExactly(1, 2));

        cancellable.cancel();

        emitter.get().emit(Message.of(3));
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertThat(received).containsExactly(1, 2);
    }

    @Test
    void toCancelStopsDelivery() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        var emitter = new java.util.concurrent.atomic.AtomicReference<io.smallrye.mutiny.subscription.MultiEmitter<? super Message<?>>>();
        Multi<Message<?>> source = Multi.createFrom().emitter(e -> emitter.set(e));

        Flow.Subscriber<? extends Message<?>> subscriber = factory.outgoing(outgoingConfig("test-out"));

        Cancellable cancellable = factory.bind(source, Integer.class)
                .process(i -> "v-" + i)
                .to(subscriber);

        await().untilAsserted(() -> assertThat(emitter.get()).isNotNull());

        emitter.get().emit(Message.of(1));
        emitter.get().emit(Message.of(2));
        await().untilAsserted(() -> assertThat(connector.list()).containsExactly("v-1", "v-2"));

        cancellable.cancel();

        emitter.get().emit(Message.of(3));
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertThat(connector.list()).containsExactly("v-1", "v-2");
    }
}
