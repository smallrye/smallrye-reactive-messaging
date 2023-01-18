package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.MqttTestBase.awaitForMosquittoToBeReady;
import static org.awaitility.Awaitility.await;

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
import io.smallrye.reactive.messaging.health.HealthReport;
import io.vertx.mutiny.core.Vertx;

public class SecureMqttTestBase {

    public static GenericContainer<?> mosquitto = new GenericContainer<>("eclipse-mosquitto:2.0")
            .withExposedPorts(1883)
            .withFileSystemBind("src/test/resources/mosquitto-secure", "/mosquitto/config", BindMode.READ_WRITE)
            .waitingFor(Wait.forListeningPort())
            .waitingFor(Wait.forLogMessage(".*mosquitto .* running.*", 1));

    Vertx vertx;
    protected String address;
    protected Integer port;
    protected MqttUsage usage;

    @BeforeAll
    public static void startBroker() {
        mosquitto.start();
        awaitForMosquittoToBeReady(mosquitto);
    }

    public void awaitUntilReady(MqttSource source) {
        await().until(() -> {
            HealthReport.HealthReportBuilder builder = HealthReport.builder();
            source.isReady(builder);
            return builder.build().isOk();
        });
    }

    @AfterAll
    public static void stopBroker() {
        mosquitto.stop();
    }

    @BeforeEach
    public void setup() {
        mosquitto.followOutput(new Slf4jLogConsumer(LoggerFactory.getLogger("mosquitto")));
        vertx = Vertx.vertx();
        address = mosquitto.getHost();
        port = mosquitto.getMappedPort(1883);
        System.setProperty("mqtt-host", address);
        System.setProperty("mqtt-port", Integer.toString(port));
        System.setProperty("mqtt-user", "user");
        System.setProperty("mqtt-pwd", "foo");
        usage = new MqttUsage(address, port, "user", "foo");
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
