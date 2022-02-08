package kafka.companion;

import java.util.Collection;

import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.test.KafkaCompanionTestBase;

public class OffsetsTest extends KafkaCompanionTestBase {

    @Test
    public void offsets() {
        // <offsets>
        TopicPartition topicPartition = KafkaCompanion.tp("topic", 1);
        long latestOffset = companion.offsets().get(topicPartition, OffsetSpec.latest()).offset();
        // </offsets>
    }

    @Test
    public void consumerGroups() {
        // <consumerGroups>
        Collection<ConsumerGroupListing> consumerGroups = companion.consumerGroups().list();
        for (ConsumerGroupListing consumerGroup : consumerGroups) {
            // check consumer groups
        }

        TopicPartition topicPartition = KafkaCompanion.tp("topic", 1);
        companion.consumerGroups().resetOffsets("consumer-group", topicPartition);
        // </consumerGroups>
    }

}
