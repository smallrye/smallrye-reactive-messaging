package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.BeanManager;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.impl.KafkaAdminHelper;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.vertx.kafka.admin.ListConsumerGroupOffsetsOptions;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;

public class KafkaCommitHandlerTest extends KafkaTestBase {

    private WeldContainer container;

    @After
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Test
    public void testSourceWithAutoCommitEnabled() throws ExecutionException, TimeoutException, InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("enable.auto.commit", "true");
        config.put("group.id", "test-source-with-auto-commit-enabled");
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(new MapBasedConfig(config));
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, "test-source-with-auto-commit-enabled", ic,
                getConsumerRebalanceListeners());

        List<Message<?>> messages = Collections.synchronizedList(new ArrayList<>());
        source.getStream().subscribe().with(messages::add);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        Optional<IncomingKafkaRecord<String, Integer>> firstMessage = messages
                .stream()
                .map(m -> (IncomingKafkaRecord<String, Integer>) m)
                .findFirst();

        assertTrue(firstMessage.isPresent());
        CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        firstMessage.get().ack().whenComplete((a, t) -> ackFuture.complete(null));
        ackFuture.get(2, TimeUnit.MINUTES);

        await().atMost(2, TimeUnit.MINUTES)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    TopicPartition topicPartition = new TopicPartition(topic, 0);
                    CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = new CompletableFuture<>();
                    KafkaAdminHelper.createAdminClient(ic, vertx, config)
                            .getDelegate()
                            .listConsumerGroupOffsets("test-source-with-auto-commit-enabled",
                                    new ListConsumerGroupOffsetsOptions()
                                            .topicPartitions(Collections.singletonList(topicPartition)),
                                    a -> {
                                        if (a.failed()) {
                                            future.completeExceptionally(a.cause());
                                        } else {
                                            future.complete(a.result());
                                        }
                                    });

                    Map<TopicPartition, OffsetAndMetadata> result = future.get();
                    assertNotNull(result.get(topicPartition));
                    assertEquals(10L, result.get(topicPartition).getOffset());
                });
    }

    @Test
    public void testSourceWithAutoCommitDisabled() throws ExecutionException, InterruptedException, TimeoutException {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("enable.auto.commit", "false");
        config.put("commit-strategy", "latest");
        config.put("group.id", "test-source-with-auto-commit-disabled");
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(new MapBasedConfig(config));
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, "test-source-with-auto-commit-disabled", ic,
                getConsumerRebalanceListeners());

        List<Message<?>> messages = Collections.synchronizedList(new ArrayList<>());
        source.getStream().subscribe().with(messages::add);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        Optional<IncomingKafkaRecord<String, Integer>> firstMessage = messages
                .stream()
                .map(m -> (IncomingKafkaRecord<String, Integer>) m)
                .findFirst();

        assertTrue(firstMessage.isPresent());
        CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        firstMessage.get().ack().whenComplete((a, t) -> ackFuture.complete(null));
        ackFuture.get(2, TimeUnit.MINUTES);

        TopicPartition topicPartition = new TopicPartition(topic, 0);
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = new CompletableFuture<>();
        KafkaAdminHelper.createAdminClient(ic, vertx, config)
                .getDelegate()
                .listConsumerGroupOffsets("test-source-with-auto-commit-disabled",
                        new ListConsumerGroupOffsetsOptions()
                                .topicPartitions(Collections.singletonList(topicPartition)),
                        a -> {
                            if (a.failed()) {
                                future.completeExceptionally(a.cause());
                            } else {
                                future.complete(a.result());
                            }
                        });

        Map<TopicPartition, OffsetAndMetadata> result = future.get();
        assertNotNull(result.get(topicPartition));
        assertEquals(10L, result.get(topicPartition).getOffset());
    }

    @Test
    public void testSourceWithThrottledLatestProcessedCommitEnabled() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("enable.auto.commit", "false");
        config.put("commit-strategy", "throttled");
        config.put("group.id", "test-source-with-throttled-latest-processed-commit");
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(new MapBasedConfig(config));
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, "test-source-with-throttled-latest-processed-commit", ic,
                getConsumerRebalanceListeners());

        List<Message<?>> messages = Collections.synchronizedList(new ArrayList<>());
        source.getStream().subscribe().with(messages::add);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    // we must keep acking to eventually induce a commit
                    messages
                            .forEach(Message::ack);

                    TopicPartition topicPartition = new TopicPartition(topic, 0);
                    CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = new CompletableFuture<>();
                    KafkaAdminHelper.createAdminClient(ic, vertx, config)
                            .getDelegate()
                            .listConsumerGroupOffsets("test-source-with-throttled-latest-processed-commit",
                                    new ListConsumerGroupOffsetsOptions()
                                            .topicPartitions(Collections.singletonList(topicPartition)),
                                    a -> {
                                        if (a.failed()) {
                                            future.completeExceptionally(a.cause());
                                        } else {
                                            future.complete(a.result());
                                        }
                                    });

                    Map<TopicPartition, OffsetAndMetadata> result = future.get();
                    assertNotNull(result.get(topicPartition));
                    assertEquals(10L, result.get(topicPartition).getOffset());
                });

        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> {
                    HealthReport.HealthReportBuilder healthReportBuilder = HealthReport.builder();
                    source.isAlive(healthReportBuilder);
                    assertTrue(healthReportBuilder.build().isOk());
                });
    }

    @Test
    public void testSourceWithThrottledLatestProcessedCommitEnabledWithoutAcking() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("enable.auto.commit", "false");
        config.put("commit-strategy", "throttled");
        config.put("max.poll.records", "16");
        config.put("group.id", "test-source-with-throttled-latest-processed-commit-without-acking");
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(new MapBasedConfig(config));
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx,
                "test-source-with-throttled-latest-processed-commit-without-acking", ic,
                getConsumerRebalanceListeners());

        List<Message<?>> messages = Collections.synchronizedList(new ArrayList<>());
        source.getStream().subscribe().with(messages::add);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> {
                    HealthReport.HealthReportBuilder healthReportBuilder = HealthReport.builder();
                    source.isAlive(healthReportBuilder);
                    assertTrue(healthReportBuilder.build().isOk());
                });

        new Thread(() -> usage.produceIntegers(30, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 30);

        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> {
                    HealthReport.HealthReportBuilder healthReportBuilder = HealthReport.builder();
                    source.isAlive(healthReportBuilder);
                    assertFalse(healthReportBuilder.build().isOk());
                });
    }

    private Map<String, Object> newCommonConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("auto.offset.reset", "earliest");
        return config;
    }

    private BeanManager getBeanManager() {
        if (container == null) {
            Weld weld = baseWeld();
            addConfig(new MapBasedConfig(new HashMap<>()));
            weld.disableDiscovery();
            container = weld.initialize();
        }
        return container.getBeanManager();
    }

    private Instance<KafkaConsumerRebalanceListener> getConsumerRebalanceListeners() {
        return getBeanManager()
                .createInstance()
                .select(KafkaConsumerRebalanceListener.class);
    }
}
