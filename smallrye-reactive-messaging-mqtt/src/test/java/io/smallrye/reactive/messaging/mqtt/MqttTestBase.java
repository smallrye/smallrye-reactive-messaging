package io.smallrye.reactive.messaging.mqtt;

import java.io.File;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.Weld;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.providers.MediatorFactory;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.providers.extension.EmitterImpl;
import io.smallrye.reactive.messaging.providers.extension.MediatorManager;
import io.smallrye.reactive.messaging.providers.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.providers.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.providers.impl.ConnectorFactories;
import io.smallrye.reactive.messaging.providers.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.providers.locals.ContextDecorator;
import io.smallrye.reactive.messaging.providers.wiring.Wiring;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class MqttTestBase {

    public static GenericContainer<?> mosquitto = new GenericContainer<>("eclipse-mosquitto:1.6")
            .withExposedPorts(1883)
            .waitingFor(Wait.forLogMessage(".*listen socket on port 1883.*\\n", 2));

    Vertx vertx;
    String address;
    Integer port;
    MqttUsage usage;

    @BeforeAll
    public static void startBroker() {
        mosquitto.start();
    }

    @AfterAll
    public static void stopBroker() {
        mosquitto.stop();
    }

    @BeforeEach
    public void setup() {
        System.clearProperty("mqtt-host");
        System.clearProperty("mqtt-port");
        System.clearProperty("mqtt-user");
        System.clearProperty("mqtt-pwd");
        vertx = Vertx.vertx();
        address = mosquitto.getContainerIpAddress();
        port = mosquitto.getMappedPort(1883);
        System.setProperty("mqtt-host", address);
        System.setProperty("mqtt-port", Integer.toString(port));
        usage = new MqttUsage(address, port);
    }

    @AfterEach
    public void tearDown() {
        System.clearProperty("mqtt-host");
        System.clearProperty("mqtt-port");
        System.clearProperty("mqtt-user");
        System.clearProperty("mqtt-pwd");

        vertx.closeAndAwait();
        usage.close();

        SmallRyeConfigProviderResolver.instance()
                .releaseConfig(ConfigProvider.getConfig(this.getClass().getClassLoader()));
    }

    static Weld baseWeld(MapBasedConfig config) {
        addConfig(config);
        Weld weld = new Weld();
        weld.disableDiscovery();
        weld.addBeanClass(MediatorFactory.class);
        weld.addBeanClass(MediatorManager.class);
        weld.addBeanClass(InternalChannelRegistry.class);
        weld.addBeanClass(ConnectorFactories.class);
        weld.addBeanClass(ConfiguredChannelFactory.class);
        weld.addBeanClass(WorkerPoolRegistry.class);
        weld.addBeanClass(ExecutionHolder.class);
        weld.addBeanClass(Wiring.class);
        weld.addPackages(EmitterImpl.class.getPackage());
        weld.addExtension(new ReactiveMessagingExtension());
        weld.addBeanClass(MqttConnector.class);
        weld.addBeanClass(ContextDecorator.class);

        // Add SmallRye Config
        weld.addExtension(new io.smallrye.config.inject.ConfigExtension());

        return weld;
    }

    static void addConfig(MapBasedConfig config) {
        if (config != null) {
            config.write();
        } else {
            clear();
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void clear() {
        File out = new File("target/test-classes/META-INF/microprofile-config.properties");
        if (out.isFile()) {
            out.delete();
        }
    }

}
