package io.smallrye.reactive.messaging.pulsar.perf;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.PulsarMessage;
import io.smallrye.reactive.messaging.pulsar.TestTags;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.pulsar.converters.PulsarMessageConverter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

/**
 * This test is intended to be used to generate flame-graphs to see where the time is spent in an end-to-end scenario.
 * It generates records to a topic.
 * Then, the application read from this topic and write to another one.
 * The test stops when an external consumers has received all the records written by the application.
 */
@Tag(TestTags.PERFORMANCE)
@Tag(TestTags.SLOW)
@Disabled
public class EndToEndPayloadPerfTest extends WeldTestBase {

    public static final int COUNT = 10_000;
    public static String input_topic = UUID.randomUUID().toString();
    public static String output_topic = UUID.randomUUID().toString();

    @BeforeAll
    static void insertRecords() throws PulsarClientException {
        send(client.newProducer(Schema.BYTES)
                .producerName("consumer-perf")
                .topic(input_topic)
                .create(), COUNT, (i, p) -> p.newMessage().key("key").value(generateRandomPayload(10000)));

    }

    private MapBasedConfig commonConfig() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.in.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.in.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.in.topic", input_topic)
                .with("mp.messaging.incoming.in.tracing-enabled", false)
                .with("mp.messaging.incoming.in.cloud-events", false)
                .with("mp.messaging.incoming.in.subscriptionInitialPosition", SubscriptionInitialPosition.Earliest)
                .with("mp.messaging.incoming.in.schema", "BYTES")
                .with("mp.messaging.outgoing.out.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.topic", output_topic)
                .with("mp.messaging.outgoing.out.serviceUrl", serviceUrl)
                .with("mp.messaging.outgoing.out.schema", "BYTES");
    }

    private void waitForOutMessages() {
        List<MessageId> messages = new CopyOnWriteArrayList<>();
        try {
            receive(client.newConsumer(Schema.BYTES)
                    .subscriptionName(topic + "-consumer-" + UUID.randomUUID())
                    .topic(output_topic)
                    .subscribe(), COUNT, m -> messages.add(m.getMessageId()));
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
        await().atMost(1, TimeUnit.MINUTES).until(() -> messages.size() >= COUNT);
    }

    @ApplicationScoped
    public static class MyNoopProcessor {
        @Incoming("in")
        @Outgoing("out")
        public PulsarMessage<byte[]> transform(PulsarMessage<byte[]> message) {
            return PulsarMessage.from(message);
        }

    }

    @ApplicationScoped
    public static class MyHardWorkerBlockingProcessor {
        @Incoming("in")
        @Outgoing("out")
        @Blocking
        public PulsarMessage<byte[]> transform(PulsarMessage<byte[]> message) {
            consumeCPU(1_000_000);
            return PulsarMessage.from(message);
        }

    }

    @ApplicationScoped
    public static class MyHardWorkerProcessor {
        @Incoming("in")
        @Outgoing("out")
        public Uni<Message<byte[]>> transform(Message<byte[]> message) {
            return Uni.createFrom().item(message)
                    .onItem().invoke(() -> consumeCPU(1_000_000))
                    .map(PulsarMessage::from);
        }

    }

    @Test
    public void test_noop_processor() {
        addBeans(PulsarMessageConverter.class);
        runApplication(commonConfig(), MyNoopProcessor.class);
        waitForOutMessages();
    }

    @Test
    public void test_hard_worker_blocking_processor() {
        addBeans(PulsarMessageConverter.class);
        runApplication(commonConfig(), MyHardWorkerBlockingProcessor.class);
        waitForOutMessages();
    }

    @Test
    public void test_hard_worker_processor() {
        addBeans(PulsarMessageConverter.class);
        runApplication(commonConfig(), MyHardWorkerProcessor.class);
        waitForOutMessages();
    }

    private static volatile long consumedCPU = System.nanoTime();

    private static final Random RANDOM = new Random();

    // Copied from BlackHole.consumeCPU
    public static void consumeCPU(long tokens) {
        long t = consumedCPU;
        for (long i = tokens; i > 0; i--) {
            t += (t * 0x5DEECE66DL + 0xBL + i) & (0xFFFFFFFFFFFFL);
        }
        if (t == 42) {
            consumedCPU += t;
        }
    }

    public static byte[] generateRandomPayload(int size) {
        byte[] ba = new byte[size];
        RANDOM.nextBytes(ba);
        return ba;
    }

}
