package io.smallrye.reactive.messaging.amqp.ssl;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.amqp.AmqpBrokerExtension;
import io.smallrye.reactive.messaging.amqp.AmqpConnector;
import io.smallrye.reactive.messaging.amqp.AmqpUsage;
import io.smallrye.reactive.messaging.amqp.ProducingBean;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class AmqpSSLSinkCDISSLContextTest {

    private static final Logger LOGGER = Logger.getLogger(AmqpSSLSinkCDISSLContextTest.class.getName());
    private static GenericContainer<?> artemis;
    private static String host;
    private static int sslPort;
    private static int testPort;
    private static final String username = "artemis";
    private static final String password = "artemis";

    protected ExecutionHolder executionHolder;
    protected AmqpUsage usage;
    private WeldContainer container;

    @BeforeAll
    public static void startBroker() {
        String imageName = System.getProperty(AmqpBrokerExtension.ARTEMIS_IMAGE_NAME_KEY,
                AmqpBrokerExtension.ARTEMIS_IMAGE_NAME);
        artemis = new GenericContainer<>(DockerImageName.parse(imageName))
                .withExposedPorts(5672, 5673)
                .withEnv("ARTEMIS_USER", username)
                .withEnv("ARTEMIS_PASSWORD", password)
                .withEnv("AMQ_ROLE", "amq")
                .withEnv("ANONYMOUS_LOGIN", "false")
                .withEnv("EXTRA_ARGS", "--http-host 0.0.0.0 --relax-jolokia")
                .withLogConsumer(of -> LOGGER.info(of.getUtf8String()))
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("ssl/broker-ssl-container.xml"),
                        "/var/lib/artemis-instance/etc-override/broker.xml")
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("ssl/server.keystore.p12"),
                        "/tmp/server.keystore.p12")
                .waitingFor(Wait.forLogMessage(".*AMQ241004.*Artemis Console available.*\\n", 1)
                        .withStartupTimeout(Duration.ofSeconds(60)));
        artemis.start();

        host = artemis.getHost();
        sslPort = artemis.getMappedPort(5672);
        testPort = artemis.getMappedPort(5673);

        System.setProperty("amqp-host", host);
        System.setProperty("amqp-port", Integer.toString(sslPort));
        System.setProperty("amqp-user", username);
        System.setProperty("amqp-pwd", password);
    }

    @AfterAll
    public static void stopBroker() {
        if (artemis != null) {
            artemis.stop();
        }
    }

    @BeforeEach
    public void setup() {
        executionHolder = new ExecutionHolder(Vertx.vertx());
        // Use the non-SSL port for test framework usage (producing/consuming test messages)
        usage = new AmqpUsage(executionHolder.vertx(), host, testPort, username, password);
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.cleanup();
    }

    @AfterEach
    public void tearDown() {
        usage.close();
        executionHolder.terminate(null);
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.cleanup();
    }

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.shutdown();
        }

        MapBasedConfig.cleanup();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());

        System.clearProperty("mp-config");
        System.clearProperty("client-options-name");
        System.clearProperty("amqp-client-options-name");
    }

    @Test
    public void testSuppliedSslContextGlobal() throws InterruptedException {
        Weld weld = new Weld();

        CountDownLatch latch = new CountDownLatch(10);
        usage.consumeIntegers("sink",
                v -> latch.countDown());

        weld.addBeanClass(ProducingBean.class);
        weld.addBeanClass(ClientSslContextBean.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.address", "sink")
                .put("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", sslPort)
                .put("mp.messaging.outgoing.sink.durable", false)
                .put("mp.messaging.outgoing.sink.tracing-enabled", false)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("amqp-use-ssl", "true")
                .put("amqp-client-ssl-context-name", "mysslcontext")
                .write();

        container = weld.initialize();

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    public void testSuppliedSslContextConnector() throws InterruptedException {
        Weld weld = new Weld();

        CountDownLatch latch = new CountDownLatch(10);
        usage.consumeIntegers("sink",
                v -> latch.countDown());

        weld.addBeanClass(ProducingBean.class);
        weld.addBeanClass(ClientSslContextBean.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.address", "sink")
                .put("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", sslPort)
                .put("mp.messaging.outgoing.sink.durable", false)
                .put("mp.messaging.outgoing.sink.tracing-enabled", false)
                .put("mp.messaging.outgoing.sink.username", username)
                .put("mp.messaging.outgoing.sink.password", password)
                .put("mp.messaging.outgoing.sink.use-ssl", "true")
                .put("mp.messaging.outgoing.sink.client-ssl-context-name", "mysslcontext")
                .write();

        container = weld.initialize();

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    }
}
