package testing;

import io.smallrye.reactive.messaging.connectors.InMemoryConnector;
import io.smallrye.reactive.messaging.connectors.InMemorySource;
import io.smallrye.reactive.messaging.connectors.InMemorySink;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.enterprise.inject.Any;
import javax.inject.Inject;

public class MyTest {

    // 1. Switch the channels to the in-memory connector:
    @BeforeAll
    public static void switchMyChannels() {
        InMemoryConnector.switchChannelToInMemory("prices", "processed-prices");
    }

    // 2. Don't forget to reset the channel after the tests:
    @AfterAll
    public static void revertMyChannels() {
        InMemoryConnector.clear();
    }

    // 3. Inject the in-memory connector in your test,
    // or use the bean manager to retrieve the instance
    @Inject @Any
    InMemoryConnector connector;

    @Test
    void test() {
        // 4. Retrieves the in-memory source to send message
        InMemorySource<Integer> prices = connector.source("prices");
        // 5. Retrieves the in-memory sink to check what is received
        InMemorySink<Integer> results = connector.sink("processed-prices");

        // 6. Send fake messages:
        prices.send(1);
        prices.send(2);
        prices.send(3);

        // 7. Check you have receives the expected messages
        Assertions.assertEquals(3, results.received().size());
    }
}
