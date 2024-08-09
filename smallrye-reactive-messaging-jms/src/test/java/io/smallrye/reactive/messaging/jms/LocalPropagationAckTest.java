package io.smallrye.reactive.messaging.jms;

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
import jakarta.jms.ConnectionFactory;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.jms.fault.JmsFailureHandler;
import io.smallrye.reactive.messaging.providers.locals.LocalContextMetadata;
import io.smallrye.reactive.messaging.support.JmsTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class LocalPropagationAckTest extends JmsTestBase {

    private WeldContainer container;

    private String destination;

    @BeforeEach
    public void initTopic(TestInfo testInfo) {
        String cn = testInfo.getTestClass().map(Class::getSimpleName).orElse(UUID.randomUUID().toString());
        String mn = testInfo.getTestMethod().map(Method::getName).orElse(UUID.randomUUID().toString());
        destination = cn + "-" + mn + "-" + UUID.randomUUID().getMostSignificantBits();
    }

    private MapBasedConfig dataconfig() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", JmsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.destination", destination)
                .with("mp.messaging.incoming.data.durable", false)
                .with("mp.messaging.incoming.data.tracing.enabled", false);
    }

    private void produceIntegers() {
        ConnectionFactory cf = container.getBeanManager().createInstance().select(ConnectionFactory.class).get();
        AtomicInteger counter = new AtomicInteger(1);
        produceIntegers(cf, destination, 5, counter::getAndIncrement);
    }

    private <T> T runApplication(MapBasedConfig config, Class<T> beanClass) {
        config.write();
        container = deploy(beanClass);

        return container.getBeanManager().createInstance().select(beanClass).get();
    }

    @Test
    public void testChannelWithAckOnMessageContext() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig(),
                IncomingChannelWithAckOnMessageContext.class);
        bean.process(i -> i + 1);

        produceIntegers();

        await().atMost(20, TimeUnit.SECONDS).until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);
    }

    @Test
    public void testChannelWithNackOnMessageContextFail() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig()
                .with("mp.messaging.incoming.data.failure-strategy", JmsFailureHandler.Strategy.FAIL),
                IncomingChannelWithAckOnMessageContext.class);
        bean.process(i -> {
            throw new RuntimeException("boom");
        });

        produceIntegers();

        await().atMost(20, TimeUnit.SECONDS).until(() -> bean.getResults().size() >= 1);
        assertThat(bean.getResults()).contains(1);
    }

    @Test
    public void testChannelWithNackOnMessageContextIgnore() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig()
                .with("mp.messaging.incoming.data.failure-strategy", JmsFailureHandler.Strategy.IGNORE),
                IncomingChannelWithAckOnMessageContext.class);
        bean.process(i -> {
            throw new RuntimeException("boom");
        });

        produceIntegers();

        await().atMost(20, TimeUnit.SECONDS).until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    public void testChannelWithNackOnMessageContextDlq() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig()
                .with("mp.messaging.incoming.data.dead-letter-queue.destination", "dlqDestination")
                .with("mp.messaging.incoming.data.failure-strategy", JmsFailureHandler.Strategy.DEAD_LETTER_QUEUE),
                IncomingChannelWithAckOnMessageContext.class);
        bean.process(i -> {
            if (i != 3) {
                return i + 1;
            }
            throw new RuntimeException("boom");
        });

        produceIntegers();

        await().atMost(20, TimeUnit.SECONDS).until(() -> bean.getResults().size() >= 4);
        assertThat(bean.getResults()).containsExactly(2, 3, 5, 6);
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
