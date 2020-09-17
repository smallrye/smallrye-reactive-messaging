package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.Multi;

public class AmqpCreditTest extends AmqpTestBase {

    private AmqpConnector provider;

    @AfterEach
    public void cleanup() {
        if (provider != null) {
            provider.terminate(null);
        }

        MapBasedConfig.clear();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Test
    public void testCreditBasedFlowControl() {
        String topic = UUID.randomUUID().toString();
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeIntegers(topic,
                v -> expected.getAndIncrement());

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndSink(topic);
        //noinspection unchecked
        Multi.createFrom().range(0, 5000)
                .map(Message::of)
                .subscribe((Subscriber<? super Message<Integer>>) sink.build());

        await()
                .atMost(Duration.ofMinutes(1))
                .until(() -> {
                    System.out.println("Expected is " + expected.get());
                    return expected.get() == 5000;
                });
        assertThat(expected).hasValue(5000);
    }

    private SubscriberBuilder<? extends Message<?>, Void> createProviderAndSink(String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConnectorFactory.CHANNEL_NAME_ATTRIBUTE, topic);
        config.put("address", topic);
        config.put("name", "the name");
        config.put("host", host);
        config.put("durable", false);
        config.put("port", port);
        config.put("username", username);
        config.put("password", password);

        this.provider = new AmqpConnector();
        provider.setup(executionHolder);
        return this.provider.getSubscriberBuilder(new MapBasedConfig(config));
    }
}
