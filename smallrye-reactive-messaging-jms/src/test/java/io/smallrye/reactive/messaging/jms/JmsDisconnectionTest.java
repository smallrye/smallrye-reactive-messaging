package io.smallrye.reactive.messaging.jms;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.Queue;

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.reactive.messaging.jms.fault.JmsDlqFailure;
import io.smallrye.reactive.messaging.jms.fault.JmsFailStop;
import io.smallrye.reactive.messaging.jms.fault.JmsIgnoreFailure;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.smallrye.reactive.messaging.providers.MediatorFactory;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.providers.extension.ChannelProducer;
import io.smallrye.reactive.messaging.providers.extension.EmitterFactoryImpl;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.providers.extension.LegacyEmitterFactoryImpl;
import io.smallrye.reactive.messaging.providers.extension.MediatorManager;
import io.smallrye.reactive.messaging.providers.extension.MutinyEmitterFactoryImpl;
import io.smallrye.reactive.messaging.providers.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.providers.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.providers.impl.ConnectorFactories;
import io.smallrye.reactive.messaging.providers.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.providers.wiring.Wiring;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.smallrye.reactive.messaging.test.common.config.SmallRyeConfigTestUtil;
import io.vertx.core.Context;

public class JmsDisconnectionTest {

    private static Network network;
    private static GenericContainer<?> artemis;
    private static ToxiproxyContainer toxiproxy;
    private static Proxy proxy;
    private static String brokerUrl;

    private static final String ARTEMIS_IMAGE = "apache/artemis:2.53.0";

    @BeforeAll
    static void startContainers() throws IOException {
        Infrastructure.setCanCallerThreadBeBlockedSupplier(() -> !Context.isOnEventLoopThread());
        network = Network.newNetwork();

        artemis = new GenericContainer<>(DockerImageName.parse(ARTEMIS_IMAGE))
                .withExposedPorts(61616)
                .withNetworkAliases("artemis")
                .withNetwork(network)
                .withEnv("ARTEMIS_USER", "artemis")
                .withEnv("ARTEMIS_PASSWORD", "artemis")
                .withEnv("ANONYMOUS_LOGIN", "true")
                .withEnv("EXTRA_ARGS", "--http-host 0.0.0.0 --relax-jolokia")
                .waitingFor(Wait.forLogMessage(".*AMQ241004.*Artemis Console available.*\\n", 1)
                        .withStartupTimeout(Duration.ofSeconds(60)));
        artemis.start();

        toxiproxy = new ToxiproxyContainer(
                DockerImageName.parse("ghcr.io/shopify/toxiproxy:latest")
                        .asCompatibleSubstituteFor("shopify/toxiproxy"))
                .withNetwork(network)
                .withNetworkAliases("toxiproxy");
        toxiproxy.start();

        ToxiproxyClient client = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        List<Integer> exposedPorts = toxiproxy.getExposedPorts();
        int toxiPort = exposedPorts.get(exposedPorts.size() - 1);
        proxy = client.createProxy("artemis", "0.0.0.0:" + toxiPort, "artemis:61616");
        brokerUrl = "tcp://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(toxiPort);
    }

    @AfterAll
    static void stopContainers() {
        if (toxiproxy != null) {
            toxiproxy.stop();
        }
        if (artemis != null) {
            artemis.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    @BeforeEach
    void setup() throws IOException {
        proxy.enable();
        SmallRyeConfigTestUtil.releaseConfig();
        MapBasedConfig.cleanup();
    }

    @AfterEach
    void tearDown() {
        SmallRyeConfigTestUtil.releaseConfig();
        MapBasedConfig.cleanup();
    }

    private String directBrokerUrl() {
        return "tcp://" + artemis.getHost() + ":" + artemis.getMappedPort(61616);
    }

    private Weld createWeld() {
        Weld weld = new Weld();
        weld.addExtension(new io.smallrye.config.inject.ConfigExtension());
        weld.addBeanClass(MediatorFactory.class);
        weld.addBeanClass(MediatorManager.class);
        weld.addBeanClass(InternalChannelRegistry.class);
        weld.addBeanClass(ConnectorFactories.class);
        weld.addBeanClass(ConfiguredChannelFactory.class);
        weld.addBeanClass(ChannelProducer.class);
        weld.addBeanClass(ExecutionHolder.class);
        weld.addBeanClass(WorkerPoolRegistry.class);
        weld.addBeanClass(HealthCenter.class);
        weld.addBeanClass(Wiring.class);
        weld.addExtension(new ReactiveMessagingExtension());
        weld.addBeanClass(EmitterFactoryImpl.class);
        weld.addBeanClass(MutinyEmitterFactoryImpl.class);
        weld.addBeanClass(LegacyEmitterFactoryImpl.class);
        weld.addBeanClass(JmsConnector.class);
        weld.addBeanClass(JmsFailStop.Factory.class);
        weld.addBeanClass(JmsIgnoreFailure.Factory.class);
        weld.addBeanClass(JmsDlqFailure.Factory.class);
        weld.addBeanClass(TestMapping.class);
        weld.addBeanClass(ProxiedConnectionFactoryBean.class);
        weld.disableDiscovery();
        return weld;
    }

    @Test
    void testSinkDisconnectionAndRecovery() throws IOException {
        new MapBasedConfig()
                .with("mp.messaging.outgoing.jms.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.jms.destination", "sink-disconnect-queue")
                .with("mp.messaging.outgoing.jms.retry.max-retries", 10)
                .with("mp.messaging.outgoing.jms.retry.initial-delay", "PT0.5S")
                .write();

        SmallRyeConfigTestUtil.installConfig();
        Weld weld = createWeld();
        weld.addBeanClass(EmitterBean.class);

        try (WeldContainer container = weld.initialize()) {
            EmitterBean emitter = container.select(EmitterBean.class).get();

            try (ActiveMQJMSConnectionFactory directFactory = new ActiveMQJMSConnectionFactory(directBrokerUrl());
                    JMSContext directCtx = directFactory.createContext()) {
                Queue queue = directCtx.createQueue("sink-disconnect-queue");
                List<String> received = new CopyOnWriteArrayList<>();
                directCtx.createConsumer(queue).setMessageListener(m -> {
                    try {
                        received.add(m.getBody(String.class));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                emitter.send("1");
                emitter.send("2");
                await().untilAsserted(() -> assertThat(received).contains("1", "2"));

                proxy.disable();
                await().pollDelay(2, SECONDS).until(() -> true);
                proxy.enable();

                emitter.send("3");
                await().atMost(15, SECONDS)
                        .untilAsserted(() -> assertThat(received).contains("1", "2", "3"));
            }
        }
    }

    @Test
    void testDirectSinkAutoRecovery() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        JsonMapping jsonMapping = new TestMapping();
        try (ActiveMQJMSConnectionFactory factory = new ActiveMQJMSConnectionFactory(brokerUrl)) {
            JmsResourceHolder<jakarta.jms.JMSProducer> holder = new JmsResourceHolder<>("jms",
                    factory::createContext);

            MapBasedConfig config = new MapBasedConfig()
                    .with("destination", "direct-recovery-queue")
                    .with("channel-name", "jms")
                    .with("retry", true)
                    .with("retry.initial-delay", "PT0.1S")
                    .with("retry.max-delay", "PT1S")
                    .with("retry.max-retries", 10)
                    .with("retry.jitter", 0.0);

            JmsSink sink = new JmsSink(holder, new JmsConnectorOutgoingConfiguration(config),
                    UnsatisfiedInstance.instance(), jsonMapping, executor);

            // Consume directly from broker
            try (ActiveMQJMSConnectionFactory directFactory = new ActiveMQJMSConnectionFactory(directBrokerUrl());
                    JMSContext directCtx = directFactory.createContext()) {
                Queue queue = directCtx.createQueue("direct-recovery-queue");
                List<String> received = new CopyOnWriteArrayList<>();
                directCtx.createConsumer(queue).setMessageListener(m -> {
                    try {
                        received.add(m.getBody(String.class));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                Flow.Subscriber<Message<?>> subscriber = sink.getSink();
                subscriber.onSubscribe(new Subscriptions.EmptySubscription());

                AtomicBoolean acked1 = new AtomicBoolean();
                subscriber.onNext(Message.of("first",
                        () -> CompletableFuture.runAsync(() -> acked1.set(true))));
                await().until(() -> received.size() >= 1);
                assertThat(acked1).isTrue();
                assertThat(received.get(0)).isEqualTo("first");

                proxy.disable();
                await().pollDelay(2, SECONDS).until(() -> true);
                holder.close();
                proxy.enable();

                AtomicBoolean acked2 = new AtomicBoolean();
                subscriber.onNext(Message.of("after-recovery",
                        () -> CompletableFuture.runAsync(() -> acked2.set(true))));

                await().atMost(10, SECONDS).until(acked2::get);
                await().until(() -> received.contains("after-recovery"));
            }
        } finally {
            executor.shutdown();
        }
    }

    @Test
    void testDirectSinkNoRecoveryWhenRetryDisabled() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        JsonMapping jsonMapping = new TestMapping();
        try (ActiveMQJMSConnectionFactory factory = new ActiveMQJMSConnectionFactory(brokerUrl)) {
            JmsResourceHolder<jakarta.jms.JMSProducer> holder = new JmsResourceHolder<>("jms",
                    factory::createContext);

            MapBasedConfig config = new MapBasedConfig()
                    .with("destination", "no-retry-queue")
                    .with("channel-name", "jms")
                    .with("retry", false);

            JmsSink sink = new JmsSink(holder, new JmsConnectorOutgoingConfiguration(config),
                    UnsatisfiedInstance.instance(), jsonMapping, executor);

            try (ActiveMQJMSConnectionFactory directFactory = new ActiveMQJMSConnectionFactory(directBrokerUrl());
                    JMSContext directCtx = directFactory.createContext()) {
                Queue queue = directCtx.createQueue("no-retry-queue");
                List<String> received = new CopyOnWriteArrayList<>();
                directCtx.createConsumer(queue).setMessageListener(m -> {
                    try {
                        received.add(m.getBody(String.class));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                Flow.Subscriber<Message<?>> subscriber = sink.getSink();
                subscriber.onSubscribe(new Subscriptions.EmptySubscription());

                AtomicBoolean acked1 = new AtomicBoolean();
                subscriber.onNext(Message.of("before-disconnect",
                        () -> CompletableFuture.runAsync(() -> acked1.set(true))));
                await().until(() -> received.size() >= 1);
                assertThat(acked1).isTrue();
                assertThat(received.get(0)).isEqualTo("before-disconnect");

                proxy.disable();
                await().pollDelay(2, SECONDS).until(() -> true);

                AtomicBoolean acked2 = new AtomicBoolean();
                subscriber.onNext(Message.of("during-outage",
                        () -> CompletableFuture.runAsync(() -> acked2.set(true))));

                await().pollDelay(3, SECONDS).until(() -> true);
                assertThat(acked2).isFalse();

                proxy.enable();

                AtomicBoolean acked3 = new AtomicBoolean();
                subscriber.onNext(Message.of("after-reconnect",
                        () -> CompletableFuture.runAsync(() -> acked3.set(true))));

                await().pollDelay(3, SECONDS)
                        .untilAsserted(() -> {
                            assertThat(acked3).isFalse();
                            assertThat(received).doesNotContain("after-reconnect");
                        });
            }
        } finally {
            executor.shutdown();
        }
    }

    @Test
    void testSourceDisconnectionAndRecovery() throws IOException {
        new MapBasedConfig()
                .with("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.jms.destination", "source-disconnect-queue")
                .with("mp.messaging.incoming.jms.retry.max-retries", 10)
                .with("mp.messaging.incoming.jms.retry.initial-delay", "PT0.5S")
                .write();

        SmallRyeConfigTestUtil.installConfig();
        Weld weld = createWeld();
        weld.addBeanClass(SourceConsumerBean.class);

        try (WeldContainer container = weld.initialize()) {
            SourceConsumerBean consumer = container.select(SourceConsumerBean.class).get();

            try (ActiveMQJMSConnectionFactory directFactory = new ActiveMQJMSConnectionFactory(directBrokerUrl());
                    JMSContext directCtx = directFactory.createContext()) {
                Queue queue = directCtx.createQueue("source-disconnect-queue");
                JMSProducer producer = directCtx.createProducer();

                producer.send(queue, "before-1");
                producer.send(queue, "before-2");
                await().untilAsserted(() -> assertThat(consumer.received()).contains("before-1", "before-2"));

                proxy.disable();
                await().pollDelay(2, SECONDS).until(() -> true);
                proxy.enable();

                producer.send(queue, "after-1");
                producer.send(queue, "after-2");
                await().atMost(15, SECONDS)
                        .untilAsserted(() -> assertThat(consumer.received()).contains("after-1", "after-2"));
            }
        }
    }

    @ApplicationScoped
    public static class ProxiedConnectionFactoryBean {
        @Produces
        ConnectionFactory factory() {
            return new ActiveMQJMSConnectionFactory(brokerUrl + "?callTimeout=2000&connectionTTL=1000");
        }
    }

    @ApplicationScoped
    public static class EmitterBean {
        @Inject
        @Channel("jms")
        Emitter<String> jms;

        public void send(String payload) {
            jms.send(payload);
        }
    }

    @ApplicationScoped
    public static class SourceConsumerBean {
        private final List<String> messages = new CopyOnWriteArrayList<>();

        @Incoming("jms")
        public void consume(String payload) {
            System.out.println("received " + payload);
            messages.add(payload);
        }

        public List<String> received() {
            return messages;
        }
    }
}
