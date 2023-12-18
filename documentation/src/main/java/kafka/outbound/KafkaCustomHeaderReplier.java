package kafka.outbound;

import java.util.Random;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.kafka.reply.KafkaRequestReply;

@ApplicationScoped
public class KafkaCustomHeaderReplier {

    Random rand = new Random();

    @Incoming("request")
    @Outgoing("reply")
    ProducerRecord<String, Integer> process(ConsumerRecord<String, String> request) {
        Header topicHeader = request.headers().lastHeader("MY_TOPIC");
        if (topicHeader == null) {
            // Skip
            return null;
        }
        String myTopic = new String(topicHeader.value());
        int generateValue = rand.nextInt(100);
        Header partitionHeader = request.headers().lastHeader("MY_PARTITION");
        if (partitionHeader == null) {
            // Propagate incoming headers, including the correlation id header
            return new ProducerRecord<>(myTopic, null, request.key(), generateValue, request.headers());
        }
        // Send the replies to extracted myTopic-myPartition
        int myPartition = KafkaRequestReply.replyPartitionFromBytes(partitionHeader.value());
        return new ProducerRecord<>(myTopic, myPartition, request.key(), generateValue, request.headers());
    }
}
