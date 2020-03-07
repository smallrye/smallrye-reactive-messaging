package io.smallrye.reactive.messaging.amqp;

import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;

import io.vertx.mutiny.core.Vertx;
import repeat.RepeatRule;

public class AmqpTestBase {

    @ClassRule
    public static GenericContainer artemis = new GenericContainer<>("vromero/activemq-artemis:2.6.1-alpine")
            .withExposedPorts(8161)
            .withExposedPorts(5672);

    @Rule
    public RepeatRule rule = new RepeatRule();

    Vertx vertx;
    String address;
    Integer port;
    AmqpUsage usage;

    @Before
    public void setup() {
        vertx = Vertx.vertx();
        address = artemis.getContainerIpAddress();
        port = artemis.getMappedPort(5672);
        System.setProperty("amqp-host", address);
        System.setProperty("amqp-port", Integer.toString(port));
        System.setProperty("amqp-user", "artemis");
        System.setProperty("amqp-pwd", "simetraehcapa");
        usage = new AmqpUsage(vertx, address, port);
    }

    @After
    public void tearDown() throws InterruptedException {
        System.clearProperty("amqp-host");
        System.clearProperty("amqp-port");

        CountDownLatch latch = new CountDownLatch(1);
        usage.close();
        vertx.close().subscribe().with(x -> latch.countDown(), f -> latch.countDown());

        latch.await();

        Thread.sleep(1000);
    }

}
