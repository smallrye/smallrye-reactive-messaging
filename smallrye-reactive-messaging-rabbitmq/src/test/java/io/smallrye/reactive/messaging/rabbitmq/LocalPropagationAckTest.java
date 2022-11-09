package io.smallrye.reactive.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.locals.LocalContextMetadata;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class LocalPropagationAckTest extends WeldTestBase {

    private MapBasedConfig dataconfig() {
        return commonConfig()
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue.name", queue)
                .with("mp.messaging.incoming.data.exchange.name", exchange)
                .with("mp.messaging.incoming.data.exchange.routing-keys", routingKeys)
                .with("mp.messaging.incoming.data.tracing.enabled", false);
    }

    private void produceIntegers() {
        AtomicInteger counter = new AtomicInteger(1);
        usage.produce(exchange, queue, routingKeys, 5, counter::getAndIncrement);
    }

    @Test
    public void testIncomingChannelWithAckOnMessageContext() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig(),
                IncomingChannelWithAckOnMessageContext.class);
        bean.setMapper(i -> i + 1);
        produceIntegers();

        await().until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);
    }

    @Test
    public void testIncomingChannelWithAckOnMessageContextAutoAck() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig()
                .with("mp.messaging.incoming.data.auto-acknowledgement", "true"),
                IncomingChannelWithAckOnMessageContext.class);
        bean.setMapper(i -> i + 1);
        produceIntegers();

        await().until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);
    }

    @Test
    public void testIncomingChannelWithNackOnMessageContextFail() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig()
                .with("mp.messaging.incoming.data.failure-strategy", "fail"),
                IncomingChannelWithAckOnMessageContext.class);
        bean.setMapper(i -> {
            throw new RuntimeException("boom");
        });
        produceIntegers();

        await().until(() -> bean.getResults().size() >= 1);
        assertThat(bean.getResults()).contains(1);
    }

    @Test
    public void testIncomingChannelWithNackOnMessageContextAccept() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig()
                .with("mp.messaging.incoming.data.failure-strategy", "accept"),
                IncomingChannelWithAckOnMessageContext.class);
        bean.setMapper(i -> {
            throw new RuntimeException("boom");
        });
        produceIntegers();

        await().until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    public void testIncomingChannelWithNackOnMessageContextReject() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig()
                .with("mp.messaging.incoming.data.failure-strategy", "reject"),
                IncomingChannelWithAckOnMessageContext.class);
        bean.setMapper(i -> {
            throw new RuntimeException("boom");
        });
        produceIntegers();

        await().until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(1, 2, 3, 4, 5);
    }

    @ApplicationScoped
    public static class IncomingChannelWithAckOnMessageContext {

        private final List<Integer> list = new CopyOnWriteArrayList<>();

        Function<Integer, Integer> mapper;

        public void setMapper(Function<Integer, Integer> mapper) {
            this.mapper = mapper;
        }

        @Incoming("data")
        @Outgoing("sink")
        public Multi<Message<String>> process(Multi<Message<String>> incoming) {
            return incoming.onItem()
                    .transformToUniAndConcatenate(msg -> Uni.createFrom()
                            .item(() -> msg.withPayload(String.valueOf(mapper.apply(Integer.parseInt(msg.getPayload())))))
                            .chain(m -> Uni.createFrom().completionStage(m.ack()).replaceWith(m))
                            .onFailure().recoverWithUni(t -> Uni.createFrom().completionStage(msg.nack(t))
                                    .onItemOrFailure().transform((unused, throwable) -> msg)));
        }

        @Incoming("sink")
        @Acknowledgment(Acknowledgment.Strategy.NONE)
        CompletionStage<Void> sink(Message<String> msg) {
            msg.getMetadata(LocalContextMetadata.class).map(LocalContextMetadata::context).ifPresent(context -> {
                if (Vertx.currentContext().getDelegate() == context) {
                    list.add(Integer.parseInt(msg.getPayload()));
                }
            });
            return CompletableFuture.completedFuture(null);
        }

        List<Integer> getResults() {
            return list;
        }
    }

}
