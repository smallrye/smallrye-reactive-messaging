package io.smallrye.reactive.messaging.mqtt;

import java.util.Properties;

import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.vertx.mutiny.core.Vertx;

public class TlsMqttTestBase {

    public static GenericContainer<?> mosquitto = new GenericContainer<>("eclipse-mosquitto:1.6")
            .withExposedPorts(8883)
            .withFileSystemBind("src/test/resources/mosquitto-tls", "/mosquitto/config", BindMode.READ_WRITE)
            .waitingFor(Wait.forLogMessage(".*listen socket on port 8883.*\\n", 2));

    Vertx vertx;
    protected String address;
    protected Integer port;
    protected MqttUsage usage;

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
        mosquitto.followOutput(new Slf4jLogConsumer(LoggerFactory.getLogger("mosquitto")));
        vertx = Vertx.vertx();
        address = mosquitto.getContainerIpAddress();
        port = mosquitto.getMappedPort(8883);
        System.setProperty("mqtt-host", address);
        System.setProperty("mqtt-port", Integer.toString(port));
        System.setProperty("mqtt-user", "user");
        System.setProperty("mqtt-pwd", "foo");
        Properties ssl = new Properties();
        ssl.setProperty("com.ibm.ssl.keyStoreType", "JKS");
        ssl.setProperty("com.ibm.ssl.keyStore", "src/test/resources/mosquitto-tls/client/client.ks");
        ssl.setProperty("com.ibm.ssl.keyStorePassword", "password");
        ssl.setProperty("com.ibm.ssl.trustStoreType", "JKS");
        ssl.setProperty("com.ibm.ssl.trustStore", "src/test/resources/mosquitto-tls/client/client.ts");
        ssl.setProperty("com.ibm.ssl.trustStorePassword", "password");
        usage = new MqttUsage(address, port, "user", "foo", ssl);
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

}
