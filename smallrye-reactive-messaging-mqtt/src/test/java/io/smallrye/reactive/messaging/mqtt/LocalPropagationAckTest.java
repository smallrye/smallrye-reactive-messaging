package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.locals.LocalContextMetadata;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class LocalPropagationAckTest extends MqttTestBase {

    private WeldContainer container;

    private String topic;
    private String clientId;

    private MapBasedConfig dataconfig() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", MqttConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", address)
                .with("mp.messaging.incoming.data.port", port)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.client-id", clientId)
                .with("mp.messaging.incoming.data.qos", 1)
                .with("mp.messaging.incoming.data.tracing.enabled", false);
    }

    private void produceIntegers() {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger counter = new AtomicInteger(1);
        usage.produceIntegers(topic, 5, latch::countDown, counter::getAndIncrement);
        try {
            latch.await(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> T runApplication(MapBasedConfig config, Class<T> beanClass) {
        Weld weld = baseWeld(config);
        weld.addBeanClass(beanClass);
        container = weld.initialize();

        waitUntilReady(container);
        return container.getBeanManager().createInstance().select(beanClass).get();
    }

    public void waitUntilReady(WeldContainer container) {
        MqttConnector connector = container.select(MqttConnector.class,
                ConnectorLiteral.of(MqttConnector.CONNECTOR_NAME)).get();
        await().until(() -> connector.getReadiness().isOk());
    }

    @BeforeEach
    public void setupTopicName() {
        topic = UUID.randomUUID().toString();
        clientId = UUID.randomUUID().toString();
    }

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        Clients.clear();
    }

    @Test
    public void testIncomingChannelWithAckOnMessageContext() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig(),
                IncomingChannelWithAckOnMessageContext.class);
        bean.process(i -> i + 1);
        produceIntegers();

        await().until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);
    }

    @Test
    public void testIncomingChannelWithNackOnMessageContextFailStop() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig(),
                IncomingChannelWithAckOnMessageContext.class);
        bean.process(i -> {
            throw new RuntimeException();
        });
        produceIntegers();

        await().until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    public void testIncomingChannelWithNackOnMessageContextIgnore() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig()
                .with("mp.messaging.incoming.data.failure-strategy", "ignore"),
                IncomingChannelWithAckOnMessageContext.class);
        bean.process(i -> {
            throw new RuntimeException();
        });
        produceIntegers();

        await().until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(1, 2, 3, 4, 5);
    }

    @ApplicationScoped
    public static class IncomingChannelWithAckOnMessageContext {

        private final List<Integer> list = new CopyOnWriteArrayList<>();

        @Inject
        @Channel("data")
        Multi<Message<byte[]>> incoming;

        public void process(Function<Integer, Integer> mapper) {
            incoming.onItem()
                    .transformToUniAndConcatenate(msg -> Uni.createFrom()
                            .item(() -> msg.withPayload(
                                    mapper.apply(Integer.parseInt(new String(msg.getPayload()))).toString().getBytes()))
                            .onFailure().recoverWithUni(t -> Uni.createFrom().completionStage(msg.nack(t))
                                    .onItemOrFailure().transform((unused, throwable) -> msg))
                            .chain(m -> Uni.createFrom().completionStage(m.ack()).replaceWith(m)))
                    .subscribe().with(m -> {
                        m.getMetadata(LocalContextMetadata.class).map(LocalContextMetadata::context).ifPresent(context -> {
                            if (Vertx.currentContext().getDelegate() == context) {
                                list.add(Integer.parseInt(new String(m.getPayload())));
                            }
                        });
                    });
        }

        List<Integer> getResults() {
            return list;
        }
    }

}
