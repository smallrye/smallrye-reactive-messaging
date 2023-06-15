package io.smallrye.reactive.messaging.pulsar.base;

import java.lang.reflect.Method;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.pulsar.ConfigResolver;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class PulsarClientBaseTest {

    public static PulsarClient client;

    public static ExecutorService executor;

    public static String serviceUrl;

    public static String httpUrl;

    public static PulsarAdmin admin;

    public String topic;

    public Vertx vertx;

    public ConfigResolver configResolver = new ConfigResolver(UnsatisfiedInstance.instance(),
            UnsatisfiedInstance.instance(), UnsatisfiedInstance.instance(), UnsatisfiedInstance.instance());

    public MapBasedConfig baseConfig() {
        return new MapBasedConfig().with("serviceUrl", serviceUrl);
    }

    @BeforeEach
    public void createVertxAndInitUsage() {
        vertx = Vertx.vertx();
    }

    @BeforeEach
    public void initTopic(TestInfo testInfo) {
        String cn = testInfo.getTestClass().map(Class::getSimpleName).orElse(UUID.randomUUID().toString());
        String mn = testInfo.getTestMethod().map(Method::getName).orElse(UUID.randomUUID().toString());
        topic = cn + "-" + mn + "-" + UUID.randomUUID().getMostSignificantBits();
    }

    @AfterAll
    static void closeCompanion() throws PulsarClientException {
        if (client != null) {
            client.close();
        }
        if (admin != null) {
            admin.close();
        }
    }

    public static <T> Multi<Message<T>> receive(Consumer<T> consumer) {
        return Multi.createBy().repeating()
                .uni(() -> Uni.createFrom().completionStage(consumer.receiveAsync()))
                .until(m -> client.isClosed())
                .runSubscriptionOn(executor);
    }

    public static <T> Multi<Messages<T>> receiveBatch(Consumer<T> consumer) {
        return Multi.createBy().repeating()
                .uni(() -> Uni.createFrom().completionStage(consumer.batchReceiveAsync()))
                .until(m -> client.isClosed())
                .runSubscriptionOn(executor);
    }

    public static <T> void receiveBatch(Consumer<T> consumer, long numberOfMessages,
            java.util.function.Consumer<Messages<T>> callback) {
        receiveBatch(consumer).select().first(numberOfMessages).subscribe().with(callback);
    }

    public static <T> void receive(Consumer<T> consumer, long numberOfMessages,
            java.util.function.Consumer<Message<T>> callback) {
        receive(consumer).select().first(numberOfMessages).subscribe().with(callback);
    }

    public static <T> Multi<MessageId> send(Producer<T> producer, Function<Integer, T> generator) {
        return Multi.createFrom().range(0, Integer.MAX_VALUE)
                .runSubscriptionOn(executor)
                .onItem().transform(generator)
                .onItem().transformToUni(v -> Uni.createFrom().completionStage(() -> producer.sendAsync(v))).merge();
    }

    public static <T> Multi<MessageId> send(Producer<T> producer,
            BiFunction<Integer, Producer<T>, TypedMessageBuilder<T>> generator) {
        return Multi.createFrom().range(0, Integer.MAX_VALUE)
                .runSubscriptionOn(executor)
                .onItem().transform(i -> generator.apply(i, producer))
                .onItem().transformToUni(m -> Uni.createFrom().completionStage(m::sendAsync)).merge();
    }

    public static <T> List<MessageId> send(Producer<T> producer, int numberOfMessages, Function<Integer, T> generator) {
        return Multi.createFrom().range(0, numberOfMessages)
                .runSubscriptionOn(executor)
                .onItem().transform(generator)
                .onItem().transformToUni(v -> Uni.createFrom().completionStage(() -> producer.sendAsync(v))).merge()
                .subscribe().asStream().collect(Collectors.toList());
    }

    public static <T> List<MessageId> send(Producer<T> producer, int numberOfMessages,
            BiFunction<Integer, Producer<T>, TypedMessageBuilder<T>> generator) {
        return Multi.createFrom().range(0, numberOfMessages)
                .runSubscriptionOn(executor)
                .onItem().transform(i -> generator.apply(i, producer))
                .onItem().transformToUni(m -> Uni.createFrom().completionStage(m::sendAsync)).merge()
                .subscribe().asStream().collect(Collectors.toList());
    }

}
