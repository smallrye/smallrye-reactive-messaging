package io.smallrye.reactive.messaging.gcp.pubsub;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import org.jboss.weld.environment.se.Weld;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import com.google.pubsub.v1.TopicName;

import io.smallrye.config.inject.ConfigExtension;
import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.extension.ChannelProducer;
import io.smallrye.reactive.messaging.extension.HealthCenter;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PubSubTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubTestBase.class);

    static final int PUBSUB_PORT = 8085;
    static final String PROJECT_ID = "my-project-id";
    static final String SUBSCRIPTION = "pubsub-subscription-test";

    static GenericContainer<?> PUBSUB_CONTAINER;

    protected PubSubConfig config;

    @BeforeAll
    public static void startPubSubContainer() {
        PUBSUB_CONTAINER = new GenericContainer<>("google/cloud-sdk:310.0.0")
                .withExposedPorts(PUBSUB_PORT)
                .withCommand("/bin/sh", "-c",
                        String.format("gcloud beta emulators pubsub start --project=%s --host-port=0.0.0.0:%d", PROJECT_ID,
                                PUBSUB_PORT))
                .withLogConsumer(new Slf4jLogConsumer(LOGGER)
                        .withSeparateOutputStreams())
                .waitingFor(new LogMessageWaitStrategy().withRegEx("(?s).*started.*$"));
        PUBSUB_CONTAINER.start();
    }

    @AfterAll
    public static void stopPubSubContainer() {
        if (PUBSUB_CONTAINER != null) {
            PUBSUB_CONTAINER.stop();
        }
    }

    public void initConfiguration(String topic) {
        config = new PubSubConfig(PROJECT_ID, topic, null, true, "localhost",
                PUBSUB_CONTAINER.getFirstMappedPort());
    }

    protected MapBasedConfig createSourceConfig(final String topic, final String subscription,
            final int containerPort) {
        final String prefix = "mp.messaging.incoming.source.";
        final Map<String, Object> config = new HashMap<>();
        config.put(prefix.concat("connector"), PubSubConnector.CONNECTOR_NAME);
        config.put(prefix.concat("topic"), topic);

        if (subscription != null) {
            config.put(prefix.concat("subscription"), subscription);
        }

        // connector properties
        config.put("gcp-pubsub-project-id", PROJECT_ID);
        config.put("mock-pubsub-topics", true);
        config.put("mock-pubsub-host", "localhost");
        config.put("mock-pubsub-port", containerPort);

        return new MapBasedConfig(config);
    }

    static Weld baseWeld() {
        final Weld weld = new Weld();

        weld.addExtension(new ConfigExtension());
        weld.addBeanClass(MediatorFactory.class);
        weld.addBeanClass(MediatorManager.class);
        weld.addBeanClass(InternalChannelRegistry.class);
        weld.addBeanClass(ConfiguredChannelFactory.class);
        weld.addBeanClass(ChannelProducer.class);
        weld.addBeanClass(ExecutionHolder.class);
        weld.addBeanClass(WorkerPoolRegistry.class);
        weld.addBeanClass(HealthCenter.class);
        weld.addExtension(new ReactiveMessagingExtension());

        weld.addBeanClass(PubSubManager.class);
        weld.addBeanClass(PubSubConnector.class);
        weld.disableDiscovery();

        return weld;
    }

    static void addConfig(final MapBasedConfig config) {
        if (config != null) {
            config.write();
        } else {
            MapBasedConfig.clear();
        }
    }

    static void clear() {
        final File out = new File("target/test-classes/META-INF/microprofile-config.properties");
        try {
            Files.deleteIfExists(out.toPath());
        } catch (final IOException e) {
            LOGGER.error("Unable to delete {}", out, e);
        }
    }

    void deleteTopicIfExists(PubSubManager manager, String topic) {
        System.out.println("Deleting topic " + TopicName.of(PROJECT_ID, topic));
        try {
            manager.topicAdminClient(config)
                    .deleteTopic(TopicName.of(PROJECT_ID, topic));
        } catch (com.google.api.gax.rpc.NotFoundException notFoundException) {
            // The topic didn't exist.
        }
    }

}
