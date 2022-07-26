package io.smallrye.reactive.messaging.kafka.client;

import static io.smallrye.reactive.messaging.kafka.base.WeldTestBase.commitHandlerFactories;
import static io.smallrye.reactive.messaging.kafka.base.WeldTestBase.failureHandlerFactories;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.test.KafkaBrokerExtension;
import io.smallrye.reactive.messaging.kafka.companion.test.KafkaBrokerExtension.KafkaBootstrapServers;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;
import mutiny.zero.flow.adapters.AdaptersToReactiveStreams;

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
    public static KafkaCompanion companion;

    // A random topic.
    public static String topic;

    @BeforeAll
    public static void init(@KafkaBootstrapServers String bootstrapServers) {
        companion = new KafkaCompanion(bootstrapServers);
        String newTopic = "tck-" + UUID.randomUUID();
        companion.topics().createAndWait(newTopic, partitions);
        topic = newTopic;
        vertx = Vertx.vertx();

        companion.produceStrings()
                .usingGenerator(i -> new ProducerRecord<>(topic, Integer.toString(i % partitions), Integer.toString(i)),
                        MESSAGE_COUNT)
                .awaitCompletion(Duration.ofSeconds(30));

    }

    @AfterAll
    public static void shutdown() {
        if (vertx != null) {
            vertx.closeAndAwait();
        }
        if (companion != null) {
            companion.close();
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

        return AdaptersToReactiveStreams.publisher(Multi.createBy().combining().streams(multi, range).asTuple()
                .map(Tuple2::getItem1)
                .invoke(IncomingKafkaRecord::ack)
                .onFailure().invoke(t -> System.out.println("Failure detected: " + t)));
    }

    protected MapBasedConfig createConsumerConfig(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, companion.getBootstrapServers());
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

        source = new KafkaSource<>(vertx, groupId, new KafkaConnectorIncomingConfiguration(config), commitHandlerFactories,
                failureHandlerFactories,
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
        return AdaptersToReactiveStreams.publisher(Multi.createFrom().failure(() -> new Exception("boom")));
    }
}
