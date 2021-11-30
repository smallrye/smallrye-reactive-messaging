package io.smallrye.reactive.messaging.kafka.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;

class ConsumerRecordConverterTest extends KafkaTestBase {

    @Test
    public void testBeanUsingConverter() {
        KafkaMapBasedConfig builder = kafkaConfig("mp.messaging.incoming.data");
        builder.put("value.deserializer", StringDeserializer.class.getName());
        builder.put("auto.offset.reset", "earliest");
        builder.put("topic", topic);

        addBeans(ConsumerRecordConverter.class, RecordConverter.class);
        MyBean bean = runApplication(builder, MyBean.class);

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
    public void testChannelBeanUsingConverter() {
        KafkaMapBasedConfig builder = kafkaConfig("mp.messaging.incoming.data");
        builder.put("value.deserializer", StringDeserializer.class.getName());
        builder.put("auto.offset.reset", "earliest");
        builder.put("topic", topic);

        addBeans(ConsumerRecordConverter.class, RecordConverter.class);
        MyChannelBean bean = runApplication(builder, MyChannelBean.class);

        bean.consume();

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
        KafkaMapBasedConfig builder = kafkaConfig("mp.messaging.incoming.data");
        builder.put("value.deserializer", StringDeserializer.class.getName());
        builder.put("auto.offset.reset", "earliest");
        builder.put("topic", topic);

        addBeans(ConsumerRecordConverter.class, RecordConverter.class);
        RecordConverterTest.MyBean bean = runApplication(builder, RecordConverterTest.MyBean.class);

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

    @ApplicationScoped
    public static class MyChannelBean {

        private final List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();

        @Inject
        @Channel("data")
        Multi<ConsumerRecord<String, String>> recordMulti;

        public void consume() {
            recordMulti.subscribe().with(records::add);
        }

        public List<ConsumerRecord<String, String>> list() {
            return records;
        }

    }

}
