package io.smallrye.reactive.messaging.kafka.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.smallrye.reactive.messaging.kafka.base.MapBasedConfig;

class ConsumerRecordConverterTest extends KafkaTestBase {

    @Test
    public void testBeanUsingConverter() {
        MapBasedConfig.Builder builder = MapBasedConfig.builder("mp.messaging.incoming.data");
        builder.put("value.deserializer", StringDeserializer.class.getName());
        builder.put("auto.offset.reset", "earliest");
        builder.put("topic", topic);

        addBeans(ConsumerRecordConverter.class, RecordConverter.class);
        MyBean bean = runApplication(builder.build(), MyBean.class);

        AtomicInteger counter = new AtomicInteger();
        usage.produceStrings(10, null,
                () -> new ProducerRecord<>(topic, counter.get() % 2 == 0 ? "key" : "k", "v-" + counter.incrementAndGet()));

        await().until(() -> bean.list().size() == 10);

        assertThat(bean.list()).hasSize(10).allSatisfy(r -> {
            assertThat(r.value()).startsWith("v-");
            assertThat(r.key()).startsWith("k");
            if (!r.key().equalsIgnoreCase("key")) {
                assertThat(r.key()).isEqualTo("k");
            }
        });
    }

    @Test
    public void testBeanUsingConverterWithNullKeyAndValue() {
        MapBasedConfig.Builder builder = MapBasedConfig.builder("mp.messaging.incoming.data");
        builder.put("value.deserializer", StringDeserializer.class.getName());
        builder.put("auto.offset.reset", "earliest");
        builder.put("topic", topic);

        addBeans(ConsumerRecordConverter.class, RecordConverter.class);
        RecordConverterTest.MyBean bean = runApplication(builder.build(), RecordConverterTest.MyBean.class);

        usage.produceStrings(10, null,
                () -> new ProducerRecord<>(topic, null, null));

        await().until(() -> bean.list().size() == 10);

        assertThat(bean.list()).hasSize(10).allSatisfy(r -> {
            assertThat(r.value()).isNull();
            assertThat(r.value()).isNull();
        });
    }

    @ApplicationScoped
    public static class MyBean {

        private final List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(ConsumerRecord<String, String> record) {
            this.records.add(record);
        }

        public List<ConsumerRecord<String, String>> list() {
            return records;
        }

    }

}
