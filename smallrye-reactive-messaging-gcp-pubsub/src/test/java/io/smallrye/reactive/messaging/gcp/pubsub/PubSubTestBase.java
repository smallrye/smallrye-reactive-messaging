package io.smallrye.reactive.messaging.gcp.pubsub;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.jboss.weld.environment.se.Weld;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

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

public class PubSubTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubTestBase.class);

    static final int PUBSUB_PORT = 8085;
    static final String PROJECT_ID = "my-project-id";

    @ClassRule
    public static final GenericContainer<?> CONTAINER = new GenericContainer<>("google/cloud-sdk:latest")
            .withExposedPorts(PUBSUB_PORT)
            .withCommand("/bin/sh", "-c",
                    String.format("gcloud beta emulators pubsub start --project=%s --host-port=0.0.0.0:%d",
                            PROJECT_ID, PUBSUB_PORT))
            .withLogConsumer(new Slf4jLogConsumer(LOGGER)
                    .withSeparateOutputStreams())
            .waitingFor(new LogMessageWaitStrategy().withRegEx("(?s).*started.*$"));

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

}
