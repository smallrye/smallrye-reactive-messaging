package io.smallrye.reactive.messaging.kafka.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.tuples.Tuple3;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;

class CustomConverterTest extends KafkaCompanionTestBase {

    @Test
    public void testBeanUsingCustomConverter() {
        KafkaMapBasedConfig builder = kafkaConfig("mp.messaging.incoming.data");
        builder.put("value.deserializer", StringDeserializer.class.getName());
        builder.put("auto.offset.reset", "earliest");
        builder.put("topic", topic);

        addBeans(ConsumerRecordConverter.class, RecordConverter.class, MyConverter.class);
        MyBean bean = runApplication(builder, MyBean.class);

        companion.produceStrings()
                .usingGenerator(i -> new ProducerRecord<>(topic, i % 2 == 0 ? "key" : "k", "v-" + i), 10);

        await().until(() -> bean.list().size() == 10);

        assertThat(bean.list()).hasSize(10).allSatisfy(r -> {
            assertThat(r.getItem1()).isEqualTo(topic);
            assertThat(r.getItem3()).startsWith("v-");
            assertThat(r.getItem2()).startsWith("k");
            if (!r.getItem2().equalsIgnoreCase("key")) {
                assertThat(r.getItem2()).isEqualTo("k");
            }
        });
    }

    @ApplicationScoped
    public static class MyConverter implements MessageConverter {

        @Override
        public boolean canConvert(Message<?> in, Type target) {
            return in.getMetadata(IncomingKafkaRecordMetadata.class).isPresent()
                    && target.equals(Tuple3.class);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public Message<?> convert(Message<?> in, Type target) {
            IncomingKafkaRecordMetadata metadata = in.getMetadata(IncomingKafkaRecordMetadata.class)
                    .orElseThrow(() -> new IllegalStateException("No Kafka metadata"));
            return in.withPayload(Tuple3.of(metadata.getTopic(), metadata.getKey(), metadata.getRecord().value()));
        }

    }

    @ApplicationScoped
    public static class MyBean {

        private final List<Tuple3<String, String, String>> records = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(Tuple3<String, String, String> record) {
            this.records.add(record);
        }

        public List<Tuple3<String, String, String>> list() {
            return records;
        }

    }

}
