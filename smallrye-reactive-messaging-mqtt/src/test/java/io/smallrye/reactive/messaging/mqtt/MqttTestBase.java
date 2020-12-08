package io.smallrye.reactive.messaging.mqtt;

import java.io.File;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.Weld;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.extension.EmitterImpl;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;
import repeat.RepeatRule;

public class MqttTestBase {

    @ClassRule
    public static GenericContainer<?> mosquitto = new GenericContainer<>("eclipse-mosquitto:1.6")
            .withExposedPorts(1883)
            .waitingFor(Wait.forLogMessage(".*listen socket on port 1883.*\\n", 2));

    Vertx vertx;
    String address;
    Integer port;
    MqttUsage usage;

    @Rule
    public RepeatRule rule = new RepeatRule();

    @Before
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

    @After
    public void tearDown() {
        System.clearProperty("mqtt-host");
        System.clearProperty("mqtt-port");
        System.clearProperty("mqtt-user");
        System.clearProperty("mqtt-pwd");

        vertx.closeAndAwait();
        usage.close();

        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig(this.getClass().getClassLoader()));
    }

    static Weld baseWeld(MapBasedConfig config) {
        addConfig(config);
        Weld weld = new Weld();
        weld.disableDiscovery();
        weld.addBeanClass(MediatorFactory.class);
        weld.addBeanClass(MediatorManager.class);
        weld.addBeanClass(InternalChannelRegistry.class);
        weld.addBeanClass(ConfiguredChannelFactory.class);
        weld.addBeanClass(WorkerPoolRegistry.class);
        weld.addBeanClass(ExecutionHolder.class);
        weld.addPackages(EmitterImpl.class.getPackage());
        weld.addExtension(new ReactiveMessagingExtension());
        weld.addBeanClass(MqttConnector.class);

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
