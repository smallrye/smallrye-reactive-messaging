package io.smallrye.reactive.messaging.kafka.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.Record;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;

class RecordConverterTest extends KafkaTestBase {

    @SuppressWarnings("unchecked")
    @Test
    public void testConverter() {
        RecordConverter converter = new RecordConverter();
        assertThat(converter.canConvert(Message.of("foo"), Record.class)).isFalse();

        IncomingKafkaRecordMetadata<String, String> metadata = mock(IncomingKafkaRecordMetadata.class);
        when(metadata.getKey()).thenReturn("key");
        Message<String> message = Message.of("foo").addMetadata(metadata);
        assertThat(converter.canConvert(message, Record.class)).isTrue();
        assertThat(converter.convert(message, Record.class)).satisfies(m -> {
            assertThat(m.getPayload()).isInstanceOf(Record.class);
            assertThat(((Record<String, String>) m.getPayload()).key()).isEqualTo("key");
            assertThat(((Record<String, String>) m.getPayload()).value()).isEqualTo("foo");
        });

        assertThat(converter.canConvert(message, KafkaRecord.class)).isFalse();

        when(metadata.getKey()).thenReturn(null);
        message = Message.of("foo").addMetadata(metadata);
        assertThat(converter.canConvert(message, Record.class)).isTrue();
        assertThat(converter.convert(message, Record.class)).satisfies(m -> {
            assertThat(m.getPayload()).isInstanceOf(Record.class);
            assertThat(((Record<String, String>) m.getPayload()).key()).isNull();
            assertThat(((Record<String, String>) m.getPayload()).value()).isEqualTo("foo");
        });
    }

    // TODO Delete once we got rid of the legacy metadata
    @Test
    public void testConverterLegacy() {
        RecordConverter converter = new RecordConverter();
        assertThat(converter.canConvert(Message.of("foo"), Record.class)).isFalse();

        io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordMetadata<String, String> metadata = mock(
                io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordMetadata.class);
        when(metadata.getKey()).thenReturn("key");
        Message<String> message = Message.of("foo").addMetadata(metadata);
        assertThat(converter.canConvert(message, Record.class)).isTrue();
        assertThat(converter.convert(message, Record.class)).satisfies(m -> {
            assertThat(m.getPayload()).isInstanceOf(Record.class);
            assertThat(((Record<String, String>) m.getPayload()).key()).isEqualTo("key");
            assertThat(((Record<String, String>) m.getPayload()).value()).isEqualTo("foo");
        });

        assertThat(converter.canConvert(message, KafkaRecord.class)).isFalse();

        when(metadata.getKey()).thenReturn(null);
        message = Message.of("foo").addMetadata(metadata);
        assertThat(converter.canConvert(message, Record.class)).isTrue();
        assertThat(converter.convert(message, Record.class)).satisfies(m -> {
            assertThat(m.getPayload()).isInstanceOf(Record.class);
            assertThat(((Record<String, String>) m.getPayload()).key()).isNull();
            assertThat(((Record<String, String>) m.getPayload()).value()).isEqualTo("foo");
        });
    }

    @Test
    public void testBeanUsingConverter() {
        KafkaMapBasedConfig.Builder builder = KafkaMapBasedConfig.builder("mp.messaging.incoming.data");
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
        KafkaMapBasedConfig.Builder builder = KafkaMapBasedConfig.builder("mp.messaging.incoming.data");
        builder.put("value.deserializer", StringDeserializer.class.getName());
        builder.put("auto.offset.reset", "earliest");
        builder.put("topic", topic);

        addBeans(ConsumerRecordConverter.class, RecordConverter.class);
        MyBean bean = runApplication(builder.build(), MyBean.class);

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

        private final List<Record<String, String>> records = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(Record<String, String> record) {
            this.records.add(record);
        }

        public List<Record<String, String>> list() {
            return records;
        }

    }

}
