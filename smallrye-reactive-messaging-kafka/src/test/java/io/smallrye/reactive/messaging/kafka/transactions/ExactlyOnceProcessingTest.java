package io.smallrye.reactive.messaging.kafka.transactions;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.assertj.core.api.Assertions;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class ExactlyOnceProcessingTest extends KafkaCompanionTestBase {

    String inTopic;
    String outTopic;

    @Test
    void testExactlyOnceProcessor() {
        inTopic = companion.topics().createAndWait(Uuid.randomUuid().toString(), 3);
        outTopic = companion.topics().createAndWait(Uuid.randomUuid().toString(), 3);
        int numberOfRecords = 10;
        MapBasedConfig config = new MapBasedConfig(producerConfig());
        config.putAll(consumerConfig());
        ExactlyOnceProcessor application = runApplication(config, ExactlyOnceProcessor.class);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(inTopic, i), numberOfRecords);

        ConsumerTask<String, Integer> records = companion.consumeIntegers()
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(outTopic, numberOfRecords)
                .awaitCompletion(Duration.ofMinutes(1));

        Assertions.assertThat(records.getRecords())
                .extracting(ConsumerRecord::value)
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates();
    }

    @ApplicationScoped
    public static class ExactlyOnceProcessor {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        @Incoming("exactly-once-consumer")
        Uni<Void> process(KafkaRecord<String, Integer> record) {
            return transaction.withTransaction(record, emitter -> {
                emitter.send(KafkaRecord.of(record.getKey(), record.getPayload()));
                return Uni.createFrom().voidItem();
            });
        }
    }

    @RepeatedTest(5)
    void testExactlyOnceProcessorWithProcessingError() {
        inTopic = companion.topics().createAndWait(Uuid.randomUuid().toString(), 3);
        outTopic = companion.topics().createAndWait(Uuid.randomUuid().toString(), 3);
        int numberOfRecords = 10;
        MapBasedConfig config = new MapBasedConfig(producerConfig());
        config.putAll(consumerConfig());
        ExactlyOnceProcessorWithProcessingError application = runApplication(config,
                ExactlyOnceProcessorWithProcessingError.class);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(inTopic, i), numberOfRecords);

        List<ConsumerRecord<String, Integer>> committed = companion.consumeIntegers()
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(outTopic, numberOfRecords)
                .awaitCompletion(Duration.ofMinutes(1))
                .getRecords();

        Assertions.assertThat(committed)
                .extracting(ConsumerRecord::value)
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates();
    }

    private KafkaMapBasedConfig producerConfig() {
        return kafkaConfig("mp.messaging.outgoing.transactional-producer")
                .with("topic", outTopic)
                .with("transactional.id", "tx-producer")
                .with("acks", "all")
                .with("value.serializer", IntegerSerializer.class.getName());
    }

    private KafkaMapBasedConfig consumerConfig() {
        return kafkaConfig("mp.messaging.incoming.exactly-once-consumer")
                .with("topic", inTopic)
                .with("group.id", "my-consumer")
                .with("commit-strategy", "ignore")
                .with("failure-strategy", "ignore")
                .with("max.poll.records", "100")
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", IntegerDeserializer.class.getName());
    }

    @ApplicationScoped
    public static class ExactlyOnceProcessorWithProcessingError {

        @Inject
        @Channel("transactional-producer")
        @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 256)
        KafkaTransactions<Integer> transaction;

        boolean error = true;

        @Incoming("exactly-once-consumer")
        Uni<Void> process(KafkaRecord<String, Integer> record) {
            return transaction.withTransaction(record, emitter -> {
                if (error && record.getPayload() == 5) {
                    error = false;
                    throw new IllegalArgumentException("Error on first try");
                }
                emitter.send(KafkaRecord.of(record.getKey(), record.getPayload()));
                return Uni.createFrom().voidItem();
            }).onFailure().recoverWithUni(t -> Uni.createFrom().completionStage(record.nack(t)));
        }
    }
}
