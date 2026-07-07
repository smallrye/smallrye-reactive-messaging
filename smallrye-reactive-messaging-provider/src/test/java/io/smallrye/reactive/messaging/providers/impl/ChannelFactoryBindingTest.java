package io.smallrye.reactive.messaging.providers.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.function.Consumer;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.ChannelFactory;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.providers.connectors.MyDummyConnector;

class ChannelFactoryBindingTest extends WeldTestBaseWithoutTails {

    private Config channelConfig(String direction, String channelName) {
        Config overall = new SmallRyeConfigBuilder()
                .withDefaultValue("mp.messaging." + direction + "." + channelName + ".connector", "dummy")
                .build();
        return ConnectorConfig.create("mp.messaging." + direction + ".", overall, channelName);
    }

    @Test
    void incomingCreatesChannelAndRegistersConnectorName() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        ChannelRegistry registry = get(ChannelRegistry.class);

        Config config = channelConfig("incoming", "test-in");
        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", config);

        assertThat(publisher).isNotNull();
        assertThat(registry.getPublishers("test-in")).isNotEmpty();
        assertThat(registry.getIncomingConnectorName("test-in")).isEqualTo("smallrye-dummy");
    }

    @Test
    void incomingChannelDeliversMessages() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");
        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", config);

        List<Object> received = new CopyOnWriteArrayList<>();
        Multi.createFrom().publisher(publisher)
                .subscribe().with(msg -> {
                    received.add(msg.getPayload());
                    msg.ack();
                });

        await().untilAsserted(() -> assertThat(received).containsExactly(2, 3, 4));
    }

    @Test
    void outgoingWithEmitterCreatesChannelAndRegistersConnectorName() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        ChannelRegistry registry = get(ChannelRegistry.class);

        Config config = channelConfig("outgoing", "test-out");
        MutinyEmitter<String> emitter = factory.outgoing("test-out", config, MutinyEmitter.class);

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

        Config config = channelConfig("outgoing", "test-out");
        Emitter<String> emitter = factory.outgoing("test-out", config, Emitter.class);

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

        Config config = channelConfig("outgoing", "test-out-src");

        Flow.Publisher<Message<?>> source = Multi.createFrom().items("a", "b", "c")
                .map(Message::of);

        factory.outgoing("test-out-src", config, source);

        assertThat(registry.getOutgoingConnectorName("test-out-src")).isEqualTo("smallrye-dummy");
        await().untilAsserted(() -> assertThat(connector.list()).containsExactly("a", "b", "c"));
    }

    @Test
    void incomingAndOutgoingOnSameChannelNameStoreSeparateConnectors() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        ChannelRegistry registry = get(ChannelRegistry.class);

        Config inConfig = channelConfig("incoming", "shared-channel");
        Config outConfig = channelConfig("outgoing", "shared-channel");

        factory.incoming("shared-channel", inConfig);
        factory.outgoing("shared-channel", outConfig, Emitter.class);

        assertThat(registry.getIncomingConnectorName("shared-channel")).isEqualTo("smallrye-dummy");
        assertThat(registry.getOutgoingConnectorName("shared-channel")).isEqualTo("smallrye-dummy");
    }

    @Test
    void bindWithPayloadConsumer() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");
        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", config);

        List<Integer> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.bind(publisher, Integer.class)
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

        Config config = channelConfig("incoming", "test-in");
        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", config);

        List<Integer> received = new CopyOnWriteArrayList<>();
        List<Metadata> metadataList = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.bind(publisher, Integer.class)
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

        Config config = channelConfig("incoming", "test-in");
        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", config);

        List<Integer> received = new CopyOnWriteArrayList<>();
        List<String> threadNames = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.bind(publisher, Integer.class)
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

        Config config = channelConfig("incoming", "test-in");
        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", config);

        List<Integer> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.bind(publisher, Integer.class)
                .subscribe(payload -> {
                    received.add(payload);
                    return Uni.createFrom().voidItem();
                });

        await().untilAsserted(() -> assertThat(received).containsExactly(2, 3, 4));
        cancellable.cancel();
    }

    @Test
    void bindWithAsyncConsumerAndMetadata() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");
        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", config);

        List<Integer> received = new CopyOnWriteArrayList<>();
        List<Metadata> metadataList = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.bind(publisher, Integer.class)
                .subscribe((payload, metadata) -> {
                    received.add(payload);
                    metadataList.add(metadata);
                    return Uni.createFrom().voidItem();
                });

        await().untilAsserted(() -> {
            assertThat(received).containsExactly(2, 3, 4);
            assertThat(metadataList).hasSize(3);
        });
        cancellable.cancel();
    }

    @Test
    void bindWithBlockingConcurrency() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");
        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", config);

        List<Integer> received = new CopyOnWriteArrayList<>();
        List<String> threadNames = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.bind(publisher, Integer.class)
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

        Config inConfig = channelConfig("incoming", "test-in");
        Config outConfig = channelConfig("outgoing", "test-out");

        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", inConfig);
        Flow.Subscriber<? extends Message<?>> subscriber = factory.outgoing("test-out", outConfig);

        factory.bind(publisher, Integer.class)
                .process(i -> "value-" + i)
                .to(subscriber);

        await().untilAsserted(() -> assertThat(connector.list()).containsExactly("value-2", "value-3", "value-4"));
    }

    @Test
    void bindForwardToOutgoingWithoutProcess() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Config inConfig = channelConfig("incoming", "test-in");
        Config outConfig = channelConfig("outgoing", "test-out");

        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", inConfig);
        Flow.Subscriber<? extends Message<?>> subscriber = factory.outgoing("test-out", outConfig);

        factory.bind(publisher).to(subscriber);

        await().untilAsserted(() -> assertThat(connector.list()).containsExactly("2", "3", "4"));
    }

    @Test
    void bindProcessWithMetadataAndForwardToOutgoing() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Config inConfig = channelConfig("incoming", "test-in");
        Config outConfig = channelConfig("outgoing", "test-out");

        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", inConfig);
        Flow.Subscriber<? extends Message<?>> subscriber = factory.outgoing("test-out", outConfig);

        factory.bind(publisher, Integer.class)
                .process((payload, metadata) -> "meta-" + payload + "-" + metadata.getClass().getSimpleName())
                .to(subscriber);

        await().untilAsserted(() -> {
            assertThat(connector.list()).hasSize(3);
            assertThat(connector.list()).allSatisfy(s -> assertThat(s).startsWith("meta-"));
        });
    }

    @Test
    void bindProcessAsUniAndForwardToOutgoing() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Config inConfig = channelConfig("incoming", "test-in");
        Config outConfig = channelConfig("outgoing", "test-out");

        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", inConfig);
        Flow.Subscriber<? extends Message<?>> subscriber = factory.outgoing("test-out", outConfig);

        factory.bind(publisher, Integer.class)
                .processAsync((payload, metadata) -> Uni.createFrom().item("async-" + payload))
                .to(subscriber);

        await().untilAsserted(() -> assertThat(connector.list()).containsExactly("async-2", "async-3", "async-4"));
    }

    @Test
    void bindBlockingProcessAndForwardToOutgoing() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Config inConfig = channelConfig("incoming", "test-in");
        Config outConfig = channelConfig("outgoing", "test-out");

        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", inConfig);
        Flow.Subscriber<? extends Message<?>> subscriber = factory.outgoing("test-out", outConfig);

        List<String> threadNames = new CopyOnWriteArrayList<>();
        factory.bind(publisher, Integer.class)
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

        Config config = channelConfig("incoming", "test-in");
        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", config);

        ExecutorService executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "custom-executor-thread"));

        List<Integer> received = new CopyOnWriteArrayList<>();
        List<String> threadNames = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.bind(publisher, Integer.class)
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

        Config config = channelConfig("incoming", "test-in");
        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", config);

        ExecutorService executor = Executors.newFixedThreadPool(3, r -> {
            Thread t = new Thread(r, "pool-thread");
            t.setDaemon(true);
            return t;
        });

        List<Integer> received = new CopyOnWriteArrayList<>();
        List<String> threadNames = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.bind(publisher, Integer.class)
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
    void bindWithBlockingAsyncConsumer() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");
        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", config);

        List<Integer> received = new CopyOnWriteArrayList<>();
        List<String> threadNames = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.bind(publisher, Integer.class)
                .blocking()
                .subscribe(payload -> {
                    received.add(payload);
                    threadNames.add(Thread.currentThread().getName());
                    return Uni.createFrom().voidItem();
                });

        await().untilAsserted(() -> assertThat(received).containsExactly(2, 3, 4));
        assertThat(threadNames).allSatisfy(name -> assertThat(name).doesNotContain("main"));
        cancellable.cancel();
    }

    @Test
    void bindProcessWithConcurrencyToOutgoing() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Config inConfig = channelConfig("incoming", "test-in");
        Config outConfig = channelConfig("outgoing", "test-out");

        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", inConfig);
        Flow.Subscriber<? extends Message<?>> subscriber = factory.outgoing("test-out", outConfig);

        factory.bind(publisher, Integer.class)
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

        Config config = channelConfig("incoming", "test-in");
        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", config);

        List<String> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.bind(publisher, Integer.class)
                .process(i -> "processed-" + i)
                .subscribe(payload -> {
                    received.add(payload);
                });

        await().untilAsserted(() -> assertThat(received).containsExactly("processed-2", "processed-3", "processed-4"));
        cancellable.cancel();
    }

    @Test
    void bindProcessAndSubscribeAsync() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");
        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", config);

        List<String> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.bind(publisher, Integer.class)
                .process(i -> "async-" + i)
                .subscribe(payload -> {
                    received.add(payload);
                    return Uni.createFrom().voidItem();
                });

        await().untilAsserted(() -> assertThat(received).containsExactly("async-2", "async-3", "async-4"));
        cancellable.cancel();
    }

    @Test
    void bindProcessAndSubscribeWithMetadata() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");
        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", config);

        List<String> received = new CopyOnWriteArrayList<>();
        List<Metadata> metadataList = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.bind(publisher, Integer.class)
                .process(i -> "meta-" + i)
                .subscribe((payload, metadata) -> {
                    received.add(payload);
                    metadataList.add(metadata);
                });

        await().untilAsserted(() -> {
            assertThat(received).containsExactly("meta-2", "meta-3", "meta-4");
            assertThat(metadataList).hasSize(3);
        });
        cancellable.cancel();
    }

    @Test
    void incomingWithPayloadTypeSubscribe() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");

        List<Integer> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", config, Integer.class)
                .subscribe(payload -> {
                    received.add(payload);
                });

        await().untilAsserted(() -> assertThat(received).containsExactly(2, 3, 4));
        cancellable.cancel();
    }

    @Test
    void incomingWithPayloadTypeProcessAndSubscribe() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");

        List<String> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", config, Integer.class)
                .process(i -> "val-" + i)
                .subscribe(payload -> {
                    received.add(payload);
                });

        await().untilAsserted(() -> assertThat(received).containsExactly("val-2", "val-3", "val-4"));
        cancellable.cancel();
    }

    @Test
    void incomingWithPayloadTypeBlockingSubscribe() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");

        List<Integer> received = new CopyOnWriteArrayList<>();
        List<String> threadNames = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", config, Integer.class)
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
    void incomingProcessToOutgoingByName() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Config inConfig = channelConfig("incoming", "test-in");
        Config outConfig = channelConfig("outgoing", "test-out");

        factory.incoming("test-in", inConfig, Integer.class)
                .process(i -> "routed-" + i)
                .to("test-out", outConfig);

        await().untilAsserted(() -> assertThat(connector.list()).containsExactly("routed-2", "routed-3", "routed-4"));
    }

    @Test
    void incomingForwardToOutgoingByName() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Config inConfig = channelConfig("incoming", "test-in");
        Config outConfig = channelConfig("outgoing", "test-out");

        factory.incoming("test-in", inConfig, Integer.class)
                .to("test-out", outConfig);

        await().untilAsserted(() -> assertThat(connector.list()).containsExactly("2", "3", "4"));
    }

    @Test
    void incomingSubscribeWithMetadata() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");

        List<Integer> received = new CopyOnWriteArrayList<>();
        List<Metadata> metadataList = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", config, Integer.class)
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
    void incomingSubscribeAsync() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");

        List<Integer> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", config, Integer.class)
                .subscribe(payload -> {
                    received.add(payload);
                    return Uni.createFrom().voidItem();
                });

        await().untilAsserted(() -> assertThat(received).containsExactly(2, 3, 4));
        cancellable.cancel();
    }

    @Test
    void incomingSubscribeAsyncWithMetadata() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");

        List<Integer> received = new CopyOnWriteArrayList<>();
        List<Metadata> metadataList = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", config, Integer.class)
                .subscribe((payload, metadata) -> {
                    received.add(payload);
                    metadataList.add(metadata);
                    return Uni.createFrom().voidItem();
                });

        await().untilAsserted(() -> {
            assertThat(received).containsExactly(2, 3, 4);
            assertThat(metadataList).hasSize(3);
        });
        cancellable.cancel();
    }

    @Test
    void incomingBlockingConcurrencySubscribe() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");

        List<Integer> received = new CopyOnWriteArrayList<>();
        List<String> threadNames = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", config, Integer.class)
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
    void incomingBlockingCustomExecutorSubscribe() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");
        ExecutorService executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "my-executor"));

        List<Integer> received = new CopyOnWriteArrayList<>();
        List<String> threadNames = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", config, Integer.class)
                .blocking(executor)
                .subscribe(payload -> {
                    received.add(payload);
                    threadNames.add(Thread.currentThread().getName());
                });

        await().untilAsserted(() -> assertThat(received).containsExactly(2, 3, 4));
        assertThat(threadNames).allSatisfy(name -> assertThat(name).isEqualTo("my-executor"));
        cancellable.cancel();
        executor.shutdown();
    }

    @Test
    void incomingBlockingProcessToOutgoingByName() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Config inConfig = channelConfig("incoming", "test-in");
        Config outConfig = channelConfig("outgoing", "test-out");

        List<String> threadNames = new CopyOnWriteArrayList<>();
        factory.incoming("test-in", inConfig, Integer.class)
                .blocking()
                .process(i -> {
                    threadNames.add(Thread.currentThread().getName());
                    return "blocked-" + i;
                })
                .to("test-out", outConfig);

        await().untilAsserted(() -> assertThat(connector.list()).containsExactly("blocked-2", "blocked-3", "blocked-4"));
        assertThat(threadNames).allSatisfy(name -> assertThat(name).doesNotContain("main"));
    }

    @Test
    void incomingBlockingConcurrencyProcessToOutgoingByName() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Config inConfig = channelConfig("incoming", "test-in");
        Config outConfig = channelConfig("outgoing", "test-out");

        factory.incoming("test-in", inConfig, Integer.class)
                .blocking(3)
                .process(i -> "par-" + i)
                .to("test-out", outConfig);

        await().untilAsserted(() -> assertThat(connector.list())
                .containsExactlyInAnyOrder("par-2", "par-3", "par-4"));
    }

    @Test
    void incomingProcessWithMetadataToOutgoingByName() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Config inConfig = channelConfig("incoming", "test-in");
        Config outConfig = channelConfig("outgoing", "test-out");

        factory.incoming("test-in", inConfig, Integer.class)
                .process((payload, metadata) -> payload * 10)
                .to("test-out", outConfig);

        await().untilAsserted(() -> assertThat(connector.list()).containsExactly("20", "30", "40"));
    }

    @Test
    void incomingProcessAsUniToOutgoingByName() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Config inConfig = channelConfig("incoming", "test-in");
        Config outConfig = channelConfig("outgoing", "test-out");

        factory.incoming("test-in", inConfig, Integer.class)
                .processAsync((payload, metadata) -> Uni.createFrom().item("uni-" + payload))
                .to("test-out", outConfig);

        await().untilAsserted(() -> assertThat(connector.list()).containsExactly("uni-2", "uni-3", "uni-4"));
    }

    @Test
    void incomingProcessAsUniAndSubscribe() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");

        List<String> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", config, Integer.class)
                .processAsync((payload, metadata) -> Uni.createFrom().item("uni-" + payload))
                .subscribe(payload -> {
                    received.add(payload);
                });

        await().untilAsserted(() -> assertThat(received).containsExactly("uni-2", "uni-3", "uni-4"));
        cancellable.cancel();
    }

    @Test
    void incomingBlockingProcessAndSubscribe() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");

        List<String> received = new CopyOnWriteArrayList<>();
        List<String> threadNames = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", config, Integer.class)
                .blocking()
                .process(i -> "bp-" + i)
                .subscribe(payload -> {
                    received.add(payload);
                    threadNames.add(Thread.currentThread().getName());
                });

        await().untilAsserted(() -> assertThat(received).containsExactly("bp-2", "bp-3", "bp-4"));
        assertThat(threadNames).allSatisfy(name -> assertThat(name).doesNotContain("main"));
        cancellable.cancel();
    }

    @Test
    void subscribeNoArgAcksMessages() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Config inConfig = channelConfig("incoming", "test-in");
        Config outConfig = channelConfig("outgoing", "test-out");

        List<Integer> received = new CopyOnWriteArrayList<>();
        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", inConfig);

        factory.bind(publisher, Integer.class)
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

        Config inConfig = channelConfig("incoming", "test-in");
        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", inConfig);

        List<Object> acked = new CopyOnWriteArrayList<>();
        Multi<Message<?>> tracked = Multi.createFrom().publisher(publisher)
                .map(msg -> msg.withAckWithMetadata(metadata -> {
                    acked.add(msg.getPayload());
                    return msg.ack();
                }));

        factory.bind(tracked).subscribe();

        await().untilAsserted(() -> assertThat(acked).containsExactly(2, 3, 4));
    }

    @Test
    void subscribeConsumerNacksOnFailure() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config inConfig = channelConfig("incoming", "test-in");
        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", inConfig);

        List<Object> nacked = new CopyOnWriteArrayList<>();
        Multi<Message<?>> tracked = Multi.createFrom().publisher(publisher)
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

        await().untilAsserted(() -> assertThat(nacked).contains(3));
    }

    @Test
    void processFailureNacksAndContinues() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();

        Config inConfig = channelConfig("incoming", "test-in");
        Config outConfig = channelConfig("outgoing", "test-out");

        List<Object> nacked = new CopyOnWriteArrayList<>();
        Multi<Message<?>> tracked = Multi.createFrom().publisher(factory.incoming("test-in", inConfig))
                .map(msg -> msg.withNackWithMetadata((e, metadata) -> {
                    nacked.add(msg.getPayload());
                    return msg.nack(e);
                }));

        Flow.Subscriber<? extends Message<?>> subscriber = factory.outgoing("test-out", outConfig);

        factory.bind(tracked, Integer.class)
                .process(i -> {
                    if (i == 3) {
                        throw new RuntimeException("fail on 3");
                    }
                    return "ok-" + i;
                })
                .to(subscriber);

        await().untilAsserted(() -> {
            assertThat(connector.list()).containsExactly("ok-2", "ok-4");
            assertThat(nacked).contains(3);
        });
    }

    @Test
    void bindWithoutTypeSubscribe() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");
        Flow.Publisher<? extends Message<?>> publisher = factory.incoming("test-in", config);

        List<Object> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.bind(publisher)
                .subscribe(payload -> {
                    received.add(payload);
                });

        await().untilAsserted(() -> assertThat(received).containsExactly(2, 3, 4));
        cancellable.cancel();
    }

    @Test
    void processAsyncAndSubscribeWithConsumer() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");

        List<String> received = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", config, Integer.class)
                .processAsync((payload, metadata) -> Uni.createFrom().item("uni-" + payload))
                .subscribe(payload -> {
                    received.add(payload);
                    return Uni.createFrom().voidItem();
                });

        await().untilAsserted(() -> assertThat(received).containsExactly("uni-2", "uni-3", "uni-4"));
        cancellable.cancel();
    }

    @Test
    void processAsyncFailureNacks() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config inConfig = channelConfig("incoming", "test-in");

        List<Object> nacked = new CopyOnWriteArrayList<>();
        Multi<Message<?>> tracked = Multi.createFrom().publisher(factory.incoming("test-in", inConfig))
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

        await().untilAsserted(() -> {
            assertThat(received).containsExactly("ok-2", "ok-4");
            assertThat(nacked).contains(3);
        });
    }

    @Test
    void incomingProcessSubscribeAsyncWithMetadata() {
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);

        Config config = channelConfig("incoming", "test-in");

        List<String> received = new CopyOnWriteArrayList<>();
        List<Metadata> metadataList = new CopyOnWriteArrayList<>();
        Cancellable cancellable = factory.incoming("test-in", config, Integer.class)
                .process(i -> "m-" + i)
                .subscribe((payload, metadata) -> {
                    received.add(payload);
                    metadataList.add(metadata);
                    return Uni.createFrom().voidItem();
                });

        await().untilAsserted(() -> {
            assertThat(received).containsExactly("m-2", "m-3", "m-4");
            assertThat(metadataList).hasSize(3);
        });
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
        // Give some time to ensure no more items are delivered
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

        Config outConfig = channelConfig("outgoing", "test-out");
        Flow.Subscriber<? extends Message<?>> subscriber = factory.outgoing("test-out", outConfig);

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
