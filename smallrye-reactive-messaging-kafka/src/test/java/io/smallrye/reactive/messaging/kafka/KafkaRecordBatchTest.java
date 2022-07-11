package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.smallrye.reactive.messaging.kafka.commit.KafkaIgnoreCommit;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;

public class KafkaRecordBatchTest {

    @Mock
    KafkaCommitHandler commitHandler;

    @Mock
    KafkaFailureHandler onNack;

    @Captor
    ArgumentCaptor<IncomingKafkaRecord<String, Integer>> captor;

    private ConsumerRecords<String, Integer> records;

    @BeforeEach
    void setupRecords() {
        MockitoAnnotations.openMocks(this);
        when(commitHandler.handle(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(onNack.handle(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        HashMap<TopicPartition, List<ConsumerRecord<String, Integer>>> consumerRecords = new HashMap<>();
        consumerRecords.put(new TopicPartition("t", 1),
                Arrays.asList(
                        new ConsumerRecord<>("t", 1, 0, "0", 0),
                        new ConsumerRecord<>("t", 1, 1, "2", 2),
                        new ConsumerRecord<>("t", 1, 2, "4", 4),
                        new ConsumerRecord<>("t", 1, 3, "6", 6)));
        consumerRecords.put(new TopicPartition("t", 2),
                Arrays.asList(
                        new ConsumerRecord<>("t", 2, 5, "1", 1),
                        new ConsumerRecord<>("t", 2, 6, "3", 3),
                        new ConsumerRecord<>("t", 2, 7, "5", 5)));
        records = new ConsumerRecords<>(consumerRecords);
    }

    @Test
    void testGetPayload() {
        IncomingKafkaRecordBatch<String, Integer> batchRecords = new IncomingKafkaRecordBatch<>(records, "test", -1,
                commitHandler, onNack, false, false);

        List<Integer> batchPayload = batchRecords.getPayload();
        assertThat(batchPayload).hasSize(7);
        assertThat(batchPayload).containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6);
    }

    @Test
    void testGetRecords() {
        IncomingKafkaRecordBatch<String, Integer> batchRecords = new IncomingKafkaRecordBatch<>(records, "test", -1,
                commitHandler, onNack, false, false);

        List<KafkaRecord<String, Integer>> batchIncomingRecords = batchRecords.getRecords();
        assertThat(batchIncomingRecords).hasSize(7);
        assertThat(batchIncomingRecords).allSatisfy(record -> {
            assertThat(record).isInstanceOf(IncomingKafkaRecord.class);
            assertThat(record.getMetadata(IncomingKafkaRecordMetadata.class)).isPresent();
        });
        assertThat(batchIncomingRecords).map(Message::getPayload).containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6);
    }

    @Test
    void testAckLatestOffsetRecords() {
        IncomingKafkaRecordBatch<String, Integer> batchRecords = new IncomingKafkaRecordBatch<>(records, "test", -1,
                commitHandler, null, false, false);

        batchRecords.ack().toCompletableFuture().join();

        verify(commitHandler, times(2)).handle(captor.capture());
        assertThat(captor.getAllValues()).hasSize(2)
                .map(IncomingKafkaRecord::getPayload).containsExactlyInAnyOrder(5, 6);
    }

    @Test
    void testNackAllRecords() {
        IncomingKafkaRecordBatch<String, Integer> batchRecords = new IncomingKafkaRecordBatch<>(records, "test", -1,
                new KafkaIgnoreCommit(), onNack, false, false);

        batchRecords.nack(new IllegalArgumentException()).toCompletableFuture().join();

        verify(onNack, times(7)).handle(captor.capture(), any(), any());
        assertThat(captor.getAllValues()).hasSize(7)
                .map(IncomingKafkaRecord::getPayload).containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6);
    }

}
