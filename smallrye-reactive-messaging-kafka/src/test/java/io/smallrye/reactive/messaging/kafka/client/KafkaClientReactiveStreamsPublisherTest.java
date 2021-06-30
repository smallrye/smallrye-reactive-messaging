package io.smallrye.reactive.messaging.kafka.client;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.base.KafkaBrokerExtension;
import io.smallrye.reactive.messaging.kafka.base.KafkaUsage;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.TestEnvironment;
import org.reactivestreams.tck.junit5.PublisherVerification;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.smallrye.reactive.messaging.kafka.base.KafkaBrokerExtension.getBootstrapServers;
import static io.smallrye.reactive.messaging.kafka.base.TopicHelpers.createNewTopic;

@ExtendWith(KafkaBrokerExtension.class)
public class KafkaClientReactiveStreamsPublisherTest
        extends PublisherVerification<IncomingKafkaRecord<String, String>> {

    public static final int MESSAGE_COUNT = 1024;
    private KafkaSource<String, String> source;
    private static final int partitions = 4;

    public KafkaClientReactiveStreamsPublisherTest() {
        super(new TestEnvironment(750));
    }

    public static Vertx vertx;
    public static KafkaUsage usage;

    // A random topic.
    public static String topic;

    @BeforeAll
    public static void init() {
        topic = createNewTopic("tck-" + UUID.randomUUID().toString(), partitions);
        usage = new KafkaUsage();
        vertx = Vertx.vertx();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicLong count = new AtomicLong();
        usage.produceStrings(MESSAGE_COUNT, latch::countDown, () -> {
            long v = count.getAndIncrement();
            return new ProducerRecord<>(topic, Long.toString(v % partitions), Long.toString(v));
        });

        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @AfterAll
    public static void shutdown() {
        if (vertx != null) {
            vertx.closeAndAwait();
        }
    }

    @AfterEach
    public void closeVertx() {
        if (source != null) {
            source.closeQuietly();
        }
    }

    @Override
    public Publisher<IncomingKafkaRecord<String, String>> createPublisher(long elements) {
        // Artificial approach to close the stream once all elements have been emitted.
        int max = (int) elements;
        if (elements == 0) {
            max = 1;
        }
        Multi<Integer> range = Multi.createFrom().range(0, max);

        Multi<IncomingKafkaRecord<String, String>> multi = createSource()
                .getStream();

        return Multi.createBy().combining().streams(multi, range).asTuple()
                .map(Tuple2::getItem1)
                .invoke(IncomingKafkaRecord::ack)
                .onFailure().invoke(t -> System.out.println("Failure detected: " + t));
    }

    protected MapBasedConfig createConsumerConfig(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(12000));
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(1000));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 3000);
        props.put("channel-name", "test");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new MapBasedConfig(props);
    }

    public KafkaSource<String, String> createSource() {
        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .put("topic", topic);

        source = new KafkaSource<>(vertx, groupId, new KafkaConnectorIncomingConfiguration(config),
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), 0);

        return source;
    }

    @Override
    public long maxElementsFromPublisher() {
        return MESSAGE_COUNT;
    }

    @Override
    public Publisher<IncomingKafkaRecord<String, String>> createFailedPublisher() {
        // TODO Create a source with a deserialization issue.
        return Multi.createFrom().failure(() -> new Exception("boom"));
    }
}
