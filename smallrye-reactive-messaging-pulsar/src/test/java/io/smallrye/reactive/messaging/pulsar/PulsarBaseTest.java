package io.smallrye.reactive.messaging.pulsar;

import java.lang.reflect.Method;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniEmitter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

@ExtendWith(PulsarBrokerExtension.class)
public class PulsarBaseTest {

    static PulsarClient client;

    static ExecutorService executor;

    static String serviceUrl;

    String topic;

    @BeforeAll
    static void init(@PulsarBrokerExtension.PulsarServiceUrl String clusterUrl) throws PulsarClientException {
        serviceUrl = clusterUrl;
        client = PulsarClient.builder()
                .serviceUrl(clusterUrl)
                .build();
        executor = Executors.newFixedThreadPool(1);
    }

    MapBasedConfig baseConfig() {
        return new MapBasedConfig().with("service-url", serviceUrl);
    }

    @BeforeEach
    public void initTopic(TestInfo testInfo) {
        String cn = testInfo.getTestClass().map(Class::getSimpleName).orElse(UUID.randomUUID().toString());
        String mn = testInfo.getTestMethod().map(Method::getName).orElse(UUID.randomUUID().toString());
        topic = cn + "-" + mn + "-" + UUID.randomUUID().getMostSignificantBits();
    }

    @AfterAll
    static void closeCompanion() throws PulsarClientException {
        client.close();
    }

    public <T> Multi<Message<T>> receive(Consumer<T> consumer) {
        return Multi.createBy().repeating()
                .uni((UniEmitter<? super Message<T>> e) -> {
                    try {
                        e.complete(consumer.receive());
                    } catch (PulsarClientException ex) {
                        e.fail(ex);
                    }
                })
                .indefinitely()
                .runSubscriptionOn(executor);
    }

    public <T> Multi<MessageId> send(Producer<T> producer, Function<Integer, T> generator) {
        return Multi.createFrom().range(0, Integer.MAX_VALUE)
                .runSubscriptionOn(executor)
                .onItem().transform(generator)
                .onItem().transformToUniAndConcatenate(value -> Uni.createFrom().<MessageId> emitter(e -> {
                    try {
                        e.complete(producer.send(value));
                    } catch (PulsarClientException ex) {
                        e.fail(ex);
                    }
                }));
    }

    public <T> void receive(Consumer<T> consumer, long numberOfMessages, java.util.function.Consumer<Message<T>> callback) {
        receive(consumer).select().first(numberOfMessages).subscribe().with(callback);
    }

    public <T> List<MessageId> send(Producer<T> producer, long numberOfMessages, Function<Integer, T> generator) {
        return send(producer, generator).select().first(numberOfMessages)
                .subscribe().asStream().collect(Collectors.toList());
    }

}
