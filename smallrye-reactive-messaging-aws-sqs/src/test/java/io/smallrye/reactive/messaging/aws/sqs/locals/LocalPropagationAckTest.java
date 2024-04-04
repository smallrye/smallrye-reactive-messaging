package io.smallrye.reactive.messaging.aws.sqs.locals;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsClientProvider;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnector;
import io.smallrye.reactive.messaging.aws.sqs.SqsTestBase;
import io.smallrye.reactive.messaging.providers.locals.LocalContextMetadata;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class LocalPropagationAckTest extends SqsTestBase {

    private MapBasedConfig dataconfig() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue);
    }

    @BeforeEach
    void setUp() {
        System.out.println(queue);
        sendMessage(createQueue(queue), 5, (i, b) -> b.messageBody(String.valueOf(i + 1)));
    }

    @Test
    public void testChannelWithAckOnMessageContext() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig(),
                IncomingChannelWithAckOnMessageContext.class);
        bean.process(i -> i + 1);
        await().until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);
    }

    @Test
    public void testChannelWithAckOnMessageContextNothingAck() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig()
                .with("mp.messaging.incoming.data.ack.delete", false),
                IncomingChannelWithAckOnMessageContext.class);
        bean.process(i -> i + 1);
        await().until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);
    }

    @ApplicationScoped
    public static class IncomingChannelWithAckOnMessageContext {

        private final List<Integer> list = new CopyOnWriteArrayList<>();

        @Inject
        @Channel("data")
        Multi<Message<String>> incoming;

        void process(Function<Integer, Integer> mapper) {
            incoming.map(msg -> msg.withPayload(mapper.apply(Integer.parseInt(msg.getPayload()))))
                    .onItem().transformToUniAndConcatenate(msg -> Uni.createFrom().item(msg)
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
