package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.Method;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.locals.LocalContextMetadata;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class LocalPropagationAckTest extends AmqpBrokerTestBase {

    private final Weld weld = new Weld();

    private WeldContainer container;

    private String address;

    @BeforeEach
    public void initTopic(TestInfo testInfo) {
        String cn = testInfo.getTestClass().map(Class::getSimpleName).orElse(UUID.randomUUID().toString());
        String mn = testInfo.getTestMethod().map(Method::getName).orElse(UUID.randomUUID().toString());
        address = cn + "-" + mn + "-" + UUID.randomUUID().getMostSignificantBits();
    }

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    private MapBasedConfig dataconfig() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.address", address)
                .with("mp.messaging.incoming.data.durable", false)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .with("mp.messaging.incoming.data.tracing.enabled", false);
    }

    private void produceIntegers() {
        AtomicInteger counter = new AtomicInteger(1);
        usage.produce(address, 5, counter::getAndIncrement);
    }

    private <T> T runApplication(MapBasedConfig config, Class<T> beanClass) {
        config.write();
        weld.addBeanClass(beanClass);
        container = weld.initialize();

        return container.getBeanManager().createInstance().select(beanClass).get();
    }

    @Test
    public void testChannelWithAckOnMessageContext() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig(),
                IncomingChannelWithAckOnMessageContext.class);
        bean.process(i -> i + 1);
        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));

        produceIntegers();

        await().atMost(20, TimeUnit.SECONDS).until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);
    }

    @Test
    public void testChannelWithNackOnMessageContextFail() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig()
                .with("mp.messaging.incoming.data.failure-strategy", "fail"),
                IncomingChannelWithAckOnMessageContext.class);
        bean.process(i -> {
            throw new RuntimeException("boom");
        });
        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));

        produceIntegers();

        await().atMost(20, TimeUnit.SECONDS).until(() -> bean.getResults().size() >= 1);
        assertThat(bean.getResults()).contains(1);
    }

    @Test
    public void testChannelWithNackOnMessageContextAccept() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig()
                .with("mp.messaging.incoming.data.failure-strategy", "accept"),
                IncomingChannelWithAckOnMessageContext.class);
        bean.process(i -> {
            throw new RuntimeException("boom");
        });
        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));

        produceIntegers();

        await().atMost(20, TimeUnit.SECONDS).until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    public void testChannelWithNackOnMessageContextModifiedFailed() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig()
                .with("mp.messaging.incoming.data.failure-strategy", "modified-failed"),
                IncomingChannelWithAckOnMessageContext.class);
        bean.process(i -> {
            throw new RuntimeException("boom");
        });
        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));

        produceIntegers();

        await().atMost(20, TimeUnit.SECONDS).until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).contains(1, 2, 3, 4, 5);
    }

    @Test
    public void testChannelWithNackOnMessageContextModifiedFailedAndUndeliverable() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig()
                .with("mp.messaging.incoming.data.failure-strategy", "modified-failed-undeliverable-here"),
                IncomingChannelWithAckOnMessageContext.class);
        bean.process(i -> {
            throw new RuntimeException("boom");
        });
        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));

        produceIntegers();

        await().atMost(20, TimeUnit.SECONDS).until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    public void testChannelWithNackOnMessageContextReject() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig()
                .with("mp.messaging.incoming.data.failure-strategy", "reject"),
                IncomingChannelWithAckOnMessageContext.class);
        bean.process(i -> {
            throw new RuntimeException("boom");
        });
        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));

        produceIntegers();

        await().atMost(20, TimeUnit.SECONDS).until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    public void testChannelWithNackOnMessageContextRelease() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig()
                .with("mp.messaging.incoming.data.failure-strategy", "release"),
                IncomingChannelWithAckOnMessageContext.class);
        bean.process(i -> {
            throw new RuntimeException("boom");
        });
        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));

        produceIntegers();

        await().atMost(20, TimeUnit.SECONDS).until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).contains(1, 2, 3, 4, 5);
    }

    @ApplicationScoped
    public static class IncomingChannelWithAckOnMessageContext {

        private final List<Integer> list = new CopyOnWriteArrayList<>();

        @Inject
        @Channel("data")
        Multi<Message<Integer>> incoming;

        void process(Function<Integer, Integer> mapper) {
            incoming.onItem()
                    .transformToUniAndConcatenate(msg -> Uni.createFrom()
                            .item(() -> msg.withPayload(mapper.apply(msg.getPayload())))
                            .chain(m -> Uni.createFrom().completionStage(m.ack()).replaceWith(m))
                            .onFailure().recoverWithUni(t -> Uni.createFrom().completionStage(msg.nack(t))
                                    .onItemOrFailure().transform((unused, throwable) -> msg)))
                    .subscribe().with(m -> {
                        m.getMetadata(LocalContextMetadata.class).map(LocalContextMetadata::context).ifPresent(context -> {
                            if (Vertx.currentContext().getDelegate() == context) {
                                list.add(m.getPayload());
                            }
                        });
                    });
        }

        public List<Integer> getResults() {
            return list;
        }
    }

}
