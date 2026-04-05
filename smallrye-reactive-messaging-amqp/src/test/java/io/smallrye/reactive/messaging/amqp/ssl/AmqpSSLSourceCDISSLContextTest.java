package io.smallrye.reactive.messaging.amqp.ssl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.inject.se.SeContainer;

import org.eclipse.microprofile.config.ConfigProvider;
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
import io.smallrye.reactive.messaging.amqp.ConsumptionBean;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class AmqpSSLSourceCDISSLContextTest {

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

    public boolean isAmqpConnectorAlive(SeContainer container) {
        HealthCenter health = container.getBeanManager().createInstance().select(HealthCenter.class).get();
        return health.getLiveness().isOk();
    }

    public boolean isAmqpConnectorReady(SeContainer container) {
        HealthCenter health = container.getBeanManager().createInstance().select(HealthCenter.class).get();
        return health.getReadiness().isOk();
    }

    @Test
    public void testSuppliedSslContextGlobal() {
        Weld weld = new Weld();

        String address = UUID.randomUUID().toString();
        weld.addBeanClass(ClientSslContextBean.class);
        weld.addBeanClass(ConsumptionBean.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.address", address)
                .with("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", sslPort)
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .with("amqp-use-ssl", "true")
                .with("amqp-client-ssl-context-name", "mysslcontext")
                .write();

        container = weld.initialize();
        await().until(() -> isAmqpConnectorAlive(container));
        await().until(() -> isAmqpConnectorReady(container));
        List<Integer> list = container.select(ConsumptionBean.class).get().getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers(address, counter::getAndIncrement);

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testSuppliedSslContextConnector() {
        Weld weld = new Weld();

        String address = UUID.randomUUID().toString();
        weld.addBeanClass(ClientSslContextBean.class);
        weld.addBeanClass(ConsumptionBean.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.address", address)
                .with("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", sslPort)
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .with("mp.messaging.incoming.data.username", username)
                .with("mp.messaging.incoming.data.password", password)
                .with("mp.messaging.incoming.data.use-ssl", "true")
                .with("mp.messaging.incoming.data.client-ssl-context-name", "mysslcontext")
                .write();

        container = weld.initialize();
        await().until(() -> isAmqpConnectorAlive(container));
        await().until(() -> isAmqpConnectorReady(container));
        List<Integer> list = container.select(ConsumptionBean.class).get().getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers(address, counter::getAndIncrement);

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }
}
