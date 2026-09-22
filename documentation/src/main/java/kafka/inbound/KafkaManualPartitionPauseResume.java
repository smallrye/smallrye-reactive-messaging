package kafka.inbound;

import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.common.TopicPartition;

import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;

@ApplicationScoped
public class KafkaManualPartitionPauseResume {

    @Inject
    KafkaClientService kafkaClientService;

    // <code>
    public void manualPartitionPauseResume() {
        KafkaConsumer<String, String> consumer = kafkaClientService.getConsumer("my-channel");
        TopicPartition tp = new TopicPartition("my-topic", 0);

        // Pause specific partitions — survives backpressure resume cycles
        consumer.pause(Set.of(tp)).await().indefinitely();

        // Check which partitions are manually paused
        Set<TopicPartition> paused = consumer.manuallyPaused();

        // Resume specific partitions
        consumer.resume(Set.of(tp)).await().indefinitely();

        // Pause all assigned partitions
        consumer.pause(Set.of()).await().indefinitely();
    }
    // </code>
}
