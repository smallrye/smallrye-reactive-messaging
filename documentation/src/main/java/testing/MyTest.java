package testing;

import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.memory.TestIncoming;
import io.smallrye.reactive.messaging.memory.TestOutgoing;
import io.smallrye.reactive.messaging.memory.TestingConnector;

// @io.quarkus.test.junit.QuarkusTest or the Junit 5 extension that allows injection in tests
public class MyTest {

    // 1. Switch the channels to the test connector:
    @BeforeAll
    public static void switchMyChannels() {
        TestingConnector.switchIncomingChannelsToTesting("prices");
        TestingConnector.switchOutgoingChannelsToTesting("processed-prices");
    }

    // 2. Don't forget to reset the channel after the tests:
    @AfterAll
    public static void revertMyChannels() {
        TestingConnector.clear();
    }

    // 3. Inject the test connector in your test,
    // or use the bean manager to retrieve the instance
    @Inject
    @Any
    TestingConnector connector;

    @Test
    void test() {
        // 4. Retrieves the incoming channel to deliver messages
        TestIncoming<Integer> prices = connector.incoming("prices");
        // 5. Retrieves the outgoing channel to verify what the app sent
        TestOutgoing<Integer> results = connector.outgoing("processed-prices");

        // 6. Deliver fake messages to the app:
        prices.deliver(1);
        prices.deliver(2);
        prices.deliver(3);

        // 7. Verify what the app sent
        Assertions.assertEquals(3, results.sent().size());
    }
}
