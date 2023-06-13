package io.smallrye.reactive.messaging.pulsar.locals;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.locals.LocalContextMetadata;
import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class LocalPropagationAckTest extends WeldTestBase {

    private MapBasedConfig dataconfig() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.data.schema", "INT32")
                .with("mp.messaging.incoming.data.subscriptionInitialPosition", "Earliest")
                .with("mp.messaging.incoming.data.topic", topic);
    }

    @BeforeEach
    void setUp() throws PulsarClientException {
        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(topic)
                .create(), 5, i -> i + 1);
    }

    @Test
    public void testChannelWithAckOnMessageContext() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig(),
                IncomingChannelWithAckOnMessageContext.class);
        bean.process(i -> i + 1);
        await().until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);
    }

    @Test
    public void testChannelWithAckOnMessageContextAckReceiptEnabled() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig()
                .with("mp.messaging.incoming.data.ackReceiptEnabled", true),
                IncomingChannelWithAckOnMessageContext.class);
        bean.process(i -> i + 1);
        await().until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);
    }

    @Test
    public void testIncomingChannelWithNackOnMessageContextFailStop() {
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig(),
                IncomingChannelWithAckOnMessageContext.class);
        bean.process(i -> {
            throw new RuntimeException("boom");
        });
        await().until(() -> bean.getResults().size() >= 5);
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

        List<Integer> getResults() {
            return list;
        }
    }

}
