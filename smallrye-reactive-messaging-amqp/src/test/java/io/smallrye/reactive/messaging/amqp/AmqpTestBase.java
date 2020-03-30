package io.smallrye.reactive.messaging.amqp;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;

import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import io.vertx.mutiny.core.Vertx;
import repeat.RepeatRule;

public class AmqpTestBase {

    @ClassRule
    public static GenericContainer<?> artemis = new GenericContainer<>("vromero/activemq-artemis:2.6.1-alpine")
            .withExposedPorts(8161)
            .withExposedPorts(5672);

    @Rule
    public RepeatRule rule = new RepeatRule();

    ExecutionHolder executionHolder;
    String address;
    Integer port;
    AmqpUsage usage;

    @Before
    public void setup() {
        executionHolder = new ExecutionHolder(Vertx.vertx());
        address = artemis.getContainerIpAddress();
        port = artemis.getMappedPort(5672);
        System.setProperty("amqp-host", address);
        System.setProperty("amqp-port", Integer.toString(port));
        System.setProperty("amqp-user", "artemis");
        System.setProperty("amqp-pwd", "simetraehcapa");
        usage = new AmqpUsage(executionHolder.vertx(), address, port);
    }

    @After
    public void tearDown() throws InterruptedException {
        System.clearProperty("amqp-host");
        System.clearProperty("amqp-port");

        usage.close();
        executionHolder.terminate(null);
    }

}
