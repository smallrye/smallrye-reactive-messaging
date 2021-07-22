package io.smallrye.reactive.messaging.kafka.perf;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.KafkaConnector;
import io.smallrye.reactive.messaging.kafka.Record;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaUsage;
import io.smallrye.reactive.messaging.kafka.converters.RecordConverter;

/**
 * This test is intended to be used to generate flame-graphs to see where the time is spent in an end-to-end scenario.
 * It generates records to a topic.
 * Then, the application read from this topic and write to another one.
 * The test stops when an external consumers has received all the records written by the application.
 */
@Disabled
public class EndToEndPerfTest extends KafkaTestBase {

    public static final int COUNT = 50_000;
    public static String input_topic = UUID.randomUUID().toString();
    public static String output_topic = UUID.randomUUID().toString();

    @BeforeAll
    static void insertRecords() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicLong count = new AtomicLong();
        KafkaUsage usage = new KafkaUsage();
        usage.produceStrings(COUNT, latch::countDown,
                () -> new ProducerRecord<>(input_topic, "key", Long.toString(count.getAndIncrement())));
        latch.await();
    }

    @ApplicationScoped
    public static class MyProcessor {
        @Incoming("in")
        @Outgoing("out")
        public Record<String, String> transform(Record<String, String> record) {
            return Record.of(record.key(), "hello-" + record.value());
        }
    }

    @Test
    public void test() throws InterruptedException {
        addBeans(RecordConverter.class);
        runApplication(new KafkaMapBasedConfig()
                .with("mp.messaging.incoming.in.connector", KafkaConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.in.topic", input_topic)
                .with("mp.messaging.incoming.in.graceful-shutdown", false)
                .with("mp.messaging.incoming.in.pause-if-no-requests", true)
                .with("mp.messaging.incoming.in.tracing-enabled", false)
                .with("mp.messaging.incoming.in.cloud-events", false)
                .with("mp.messaging.incoming.in.auto.offset.reset", "earliest")
                .with("mp.messaging.incoming.in.bootstrap.servers", getBootstrapServers())
                .with("mp.messaging.incoming.in.value.deserializer", StringDeserializer.class.getName())
                .with("mp.messaging.incoming.in.key.deserializer", StringDeserializer.class.getName())

                .with("mp.messaging.outgoing.out.connector", KafkaConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.topic", output_topic)
                .with("mp.messaging.outgoing.out.value.serializer", StringSerializer.class.getName())
                .with("mp.messaging.outgoing.out.key.serializer", StringSerializer.class.getName())
                .with("mp.messaging.outgoing.out.bootstrap.servers", getBootstrapServers()),
                MyProcessor.class);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", getBootstrapServers());
        properties.put("group.id", UUID.randomUUID().toString());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties, new StringDeserializer(),
                new StringDeserializer());
        consumer.subscribe(Collections.singletonList(output_topic));
        boolean done = false;
        long received = 0;
        while (!done) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            received = received + records.count();
            if (received == COUNT) {
                done = true;
            }
        }
    }

}
