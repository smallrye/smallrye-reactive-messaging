package io.smallrye.reactive.messaging.mqtt;

import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.vertx.mutiny.core.Vertx;

public class SecureMqttTestBase {

    @ClassRule
    public static GenericContainer<?> mosquitto = new GenericContainer<>("eclipse-mosquitto:1.6.7")
            .withExposedPorts(1883)
            .withFileSystemBind("src/test/resources/mosquitto-secure", "/mosquitto/config", BindMode.READ_WRITE)
            .waitingFor(Wait.forLogMessage(".*listen socket on port 1883.*\\n", 2));

    Vertx vertx;
    protected String address;
    protected Integer port;
    protected MqttUsage usage;

    @Before
    public void setup() {
        mosquitto.followOutput(new Slf4jLogConsumer(LoggerFactory.getLogger("mosquitto")));
        vertx = Vertx.vertx();
        address = mosquitto.getContainerIpAddress();
        port = mosquitto.getMappedPort(1883);
        System.setProperty("mqtt-host", address);
        System.setProperty("mqtt-port", Integer.toString(port));
        System.setProperty("mqtt-user", "user");
        System.setProperty("mqtt-pwd", "foo");
        usage = new MqttUsage(address, port, "user", "foo");
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

}
