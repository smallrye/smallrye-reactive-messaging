package io.smallrye.reactive.messaging.amqp;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

public class AmqpBrokerTestBase extends AmqpBrokerHolder {

    @BeforeAll
    public static void startBroker() {
        startBroker(null);
    }

    @AfterAll
    public static void stopBroker() {
        AmqpBrokerHolder.stopBroker();
    }

    @BeforeEach
    public void setup() {
        super.setup();
    }

    @AfterEach
    public void tearDown() {
        super.tearDown();
    }

}
