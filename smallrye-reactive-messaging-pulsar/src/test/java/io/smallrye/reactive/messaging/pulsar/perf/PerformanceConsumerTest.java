package io.smallrye.reactive.messaging.pulsar.perf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.TestTags;
import io.smallrye.reactive.messaging.pulsar.ack.PulsarCumulativeAck;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

@Tag(TestTags.PERFORMANCE)
public class PerformanceConsumerTest extends WeldTestBase {

    public static final int TIMEOUT_IN_SECONDS = 400;
    public static final int COUNT = 100_000;

    public static String topic = UUID.randomUUID().toString();
    private static ArrayList<String> expected;

    @BeforeAll
    static void insertRecords() throws PulsarClientException {
        expected = new ArrayList<>();
        long start = System.currentTimeMillis();

        send(client.newProducer(Schema.STRING)
                .producerName("consumer-perf")
                .topic(topic)
                .create(), COUNT, i -> {
                    expected.add(Integer.toString(i));
                    return Integer.toString(i);
                });
        long end = System.currentTimeMillis();
        System.out.println("Published records in " + (end - start) + " ms");
    }

    MapBasedConfig commonConfig() {
        return commonConfig("ack");
    }

    MapBasedConfig commonConfig(String ackStrategy) {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.data.subscriptionInitialPosition", SubscriptionInitialPosition.Earliest)
                .with("mp.messaging.incoming.data.subscriptionType", SubscriptionType.Failover)
                .with("mp.messaging.incoming.data.acknowledgementsGroupTimeMicros", 5000)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .with("mp.messaging.incoming.data.schema", "STRING")
                .with("mp.messaging.incoming.data.ack-strategy", ackStrategy)
                .with("mp.messaging.incoming.data.numIoThreads", 16)
                .with("mp.messaging.incoming.data.receiverQueueSize", 10000)
                .with("mp.messaging.incoming.data.connectionsPerBroker", 8);
    }

    @Test
    public void testWithPostAckMessageAck() {
        MyConsumerUsingPostAck application = runApplication(commonConfig(),
                MyConsumerUsingPostAck.class);
        await()
                .atMost(Duration.ofSeconds(TIMEOUT_IN_SECONDS))
                .until(() -> application.getCount() >= COUNT);

        long start = application.getStart();
        long end = System.currentTimeMillis();

        assertThat(application.get()).containsSequence(expected);

        System.out.println("Post-Ack / Ack - Estimate: " + (end - start) + " ms");
    }

    @Test
    public void testWithPostAckCumulative() {
        addBeans(PulsarCumulativeAck.Factory.class);
        MyConsumerUsingPostAck application = runApplication(commonConfig("cumulative"),
                MyConsumerUsingPostAck.class);
        await()
                .atMost(Duration.ofSeconds(TIMEOUT_IN_SECONDS))
                .until(() -> application.getCount() >= COUNT);

        long start = application.getStart();
        long end = System.currentTimeMillis();

        assertThat(application.get()).containsSequence(expected);

        System.out.println("Post-Ack / Cumulative - Estimate: " + (end - start) + " ms");
    }

    @ApplicationScoped
    public static class MyConsumerUsingPostAck {

        LongAdder count = new LongAdder();
        List<String> list = new ArrayList<>();

        long start;

        @Incoming("data")
        public void consume(String message) {
            list.add(message);
            if (count.longValue() == 0L) {
                start = System.currentTimeMillis();
            }
            count.increment();
        }

        public List<String> get() {
            return list;
        }

        public long getCount() {
            return count.longValue();
        }

        public long getStart() {
            return start;
        }
    }

}
