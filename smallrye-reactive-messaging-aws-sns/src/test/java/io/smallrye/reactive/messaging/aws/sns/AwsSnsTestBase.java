package io.smallrye.reactive.messaging.aws.sns;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.jboss.logging.Logger;
import org.jboss.weld.environment.se.Weld;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.testcontainers.containers.GenericContainer;

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
import io.vertx.mutiny.core.Vertx;

public class AwsSnsTestBase {

    private static final Logger LOGGER = Logger.getLogger(AwsSnsTest.class);

    ExecutionHolder executionHolder;
    private String ip;
    private int port;

    @ClassRule
    public static final GenericContainer<?> CONTAINER = new GenericContainer<>("s12v/sns")
            .withExposedPorts(9911);

    @Before
    public void setup() {
        executionHolder = new ExecutionHolder(Vertx.vertx());
        ip = CONTAINER.getContainerIpAddress();
        port = CONTAINER.getMappedPort(9911);
        LOGGER.debugf("Container IP [%s] port [%d]", ip, port);
    }

    int port() {
        return port;
    }

    String ip() {
        return ip;
    }

    @After
    public void tearDown() {
        executionHolder.terminate(null);
    }

    static Weld baseWeld() {
        Weld weld = new Weld();
        // SmallRye config
        ConfigExtension extension = new ConfigExtension();
        weld.addExtension(extension);

        weld.addBeanClass(MediatorFactory.class);
        weld.addBeanClass(MediatorManager.class);
        weld.addBeanClass(InternalChannelRegistry.class);
        weld.addBeanClass(ConfiguredChannelFactory.class);
        weld.addBeanClass(ChannelProducer.class);
        weld.addBeanClass(ExecutionHolder.class);
        weld.addBeanClass(WorkerPoolRegistry.class);
        weld.addBeanClass(HealthCenter.class);
        weld.addExtension(new ReactiveMessagingExtension());

        weld.addBeanClass(SnsConnector.class);
        weld.disableDiscovery();

        return weld;
    }

    static void addConfig(MapBasedConfig config) {
        if (config != null) {
            config.write();
        } else {
            MapBasedConfig.clear();
        }
    }

    static void clear() {
        File out = new File("target/test-classes/META-INF/microprofile-config.properties");
        try {
            Files.deleteIfExists(out.toPath());
        } catch (IOException e) {
            LOGGER.error("Unable to delete {}", out, e);
        }
    }
}
