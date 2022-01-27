package kafka.companion;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.companion.test.KafkaCompanionTestBase;

public class TopicsTest extends KafkaCompanionTestBase {

    @Test
    public void topics() {
        // <code>
        companion.topics().create("topic1", 1);
        companion.topics().createAndWait("topic2", 3);
        Assertions.assertEquals(companion.topics().list().size(), 2);

        companion.topics().delete("topic1");
        // </code>
    }
}
