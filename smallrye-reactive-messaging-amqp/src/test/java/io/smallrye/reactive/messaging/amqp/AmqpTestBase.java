package io.smallrye.reactive.messaging.amqp;

import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import io.vertx.mutiny.core.Vertx;
import repeat.RepeatRule;

public class AmqpTestBase {

    @ClassRule
    public static GenericContainer<?> artemis = new GenericContainer<>("vromero/activemq-artemis:2.11.0-alpine")
            .withClasspathResourceMapping("brokers/broker-people-queue.xml", "/var/lib/artemis/etc-override/broker-0.xml",
                    BindMode.READ_ONLY)
            .withExposedPorts(8161)
            .withExposedPorts(5672);

    @Rule
    public RepeatRule rule = new RepeatRule();

    ExecutionHolder executionHolder;
    String host;
    Integer port;
    String username = "artemis";
    String password = "simetraehcapa";
    AmqpUsage usage;

    @Before
    public void setup() {
        executionHolder = new ExecutionHolder(Vertx.vertx());
        host = artemis.getContainerIpAddress();
        port = artemis.getMappedPort(5672);
        System.setProperty("amqp-host", host);
        System.setProperty("amqp-port", Integer.toString(port));
        System.setProperty("amqp-user", username);
        System.setProperty("amqp-pwd", password);
        usage = new AmqpUsage(executionHolder.vertx(), host, port);
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.clear();
    }

    @After
    public void tearDown() {
        System.clearProperty("amqp-host");
        System.clearProperty("amqp-port");

        usage.close();
        executionHolder.terminate(null);
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.clear();
    }

}
