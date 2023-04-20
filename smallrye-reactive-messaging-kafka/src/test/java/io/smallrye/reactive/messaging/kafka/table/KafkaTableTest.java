package io.smallrye.reactive.messaging.kafka.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.Table;
import io.smallrye.reactive.messaging.kafka.ConsumerRecordKeyValueExtractor;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.converters.ConsumerRecordConverter;

public class KafkaTableTest extends KafkaCompanionTestBase {

    @Test
    public void testBeanUsingTable() {
        KafkaMapBasedConfig builder = kafkaConfig("mp.messaging.incoming.data");
        builder.put("value.deserializer", StringDeserializer.class.getName());
        builder.put("auto.offset.reset", "earliest");
        builder.put("topic", topic);
        builder.withPrefix("mp.messaging.incoming.table");
        builder.put("value.deserializer", StringDeserializer.class.getName());
        builder.put("topic", topic + "-table");
        builder.put("auto.offset.reset", "earliest");

        addBeans(ConsumerRecordKeyValueExtractor.class, ConsumerRecordConverter.class);
        MyBean bean = runApplication(builder, MyBean.class);

        companion.produceStrings().fromRecords(
                new ProducerRecord<>(topic + "-table", "k", "1"),
                new ProducerRecord<>(topic + "-table", "k", "2"),
                new ProducerRecord<>(topic + "-table", "key", "3"),
                new ProducerRecord<>(topic + "-table", "key", "4"));

        companion.produceStrings()
                .usingGenerator(i -> new ProducerRecord<>(topic, i % 2 == 0 ? "key" : "k", "v-" + i), 10);

        await().until(() -> bean.list().size() == 10);

        assertThat(bean.list()).hasSize(10).allSatisfy(r -> {
            assertThat(r.value()).startsWith("v-");
            assertThat(r.key()).startsWith("k");
            if (!r.key().equalsIgnoreCase("key")) {
                assertThat(r.key()).isEqualTo("k");
            }
        });
    }

    @ApplicationScoped
    public static class MyBean {

        private final List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();

        @Inject
        @Channel("table")
        Table<String, String> myTable;

        @Incoming("data")
        public void consume(ConsumerRecord<String, String> record) {
            String s = myTable.get(record.key());
            System.out.println(s + " ");
            this.records.add(record);
        }

        public List<ConsumerRecord<String, String>> list() {
            return records;
        }

    }
}
