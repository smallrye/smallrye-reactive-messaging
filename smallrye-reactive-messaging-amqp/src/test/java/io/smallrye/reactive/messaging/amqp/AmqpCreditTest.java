package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.Vertx;

public class AmqpCreditTest extends AmqpTestBase {

    private AmqpConnector provider;
    private MockServer server;

    @AfterEach
    public void cleanup() {
        if (provider != null) {
            provider.terminate(null);
        }

        if (server != null) {
            server.close();
        }
    }

    @Test
    @Timeout(30)
    public void testCreditBasedFlowControl() throws Exception {
        int msgCount = 5000;
        CountDownLatch msgsReceived = new CountDownLatch(msgCount);
        List<Object> payloadsReceived = new ArrayList<>(msgCount);

        server = setupMockServer(msgCount, msgsReceived, payloadsReceived, executionHolder.vertx().getDelegate());

        SubscriberBuilder<? extends Message<?>, Void> sink = createProviderAndSink(UUID.randomUUID().toString(),
                server.actualPort());
        //noinspection unchecked
        Multi.createFrom().range(0, msgCount)
                .map(Message::of)
                .subscribe((Subscriber<? super Message<Integer>>) sink.build());

        assertThat(msgsReceived.await(20, TimeUnit.SECONDS))
                .withFailMessage("Sent %s msgs but %s remain outstanding", msgCount, msgsReceived.getCount()).isTrue();
        List<Integer> expectedPayloads = IntStream.range(0, msgCount).mapToObj(Integer::valueOf).collect(Collectors.toList());
        assertThat(payloadsReceived).containsAll(expectedPayloads);
    }

    private SubscriberBuilder<? extends Message<?>, Void> createProviderAndSink(String topic, int port) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConnectorFactory.CHANNEL_NAME_ATTRIBUTE, topic);
        config.put("address", topic);
        config.put("name", "the name");
        config.put("host", "localhost");
        config.put("port", port);

        this.provider = new AmqpConnector();
        provider.setup(executionHolder);

        return provider.getSubscriberBuilder(new MapBasedConfig(config));
    }

    private MockServer setupMockServer(int msgCount, CountDownLatch latch, List<Object> payloads, Vertx vertx)
            throws Exception {
        assertThat(msgCount % 10 == 0).isTrue();
        int creditBatch = msgCount / 10;

        return new MockServer(vertx, serverConnection -> {
            serverConnection.openHandler(serverSender -> {
                serverConnection.closeHandler(x -> serverConnection.close());
                serverConnection.open();
            });

            serverConnection.sessionOpenHandler(serverSession -> {
                serverSession.closeHandler(x -> serverSession.close());
                serverSession.open();
            });

            serverConnection.receiverOpenHandler(serverReceiver -> {
                // Disable the default 'prefetch' credit handling, do it ourselves in batches as used up
                serverReceiver.setPrefetch(0);

                serverReceiver.handler((delivery, message) -> {
                    Section body = message.getBody();
                    if (body instanceof AmqpValue) {
                        payloads.add(((AmqpValue) body).getValue());
                    } else {
                        payloads.add(body);
                    }

                    latch.countDown();

                    // Previous credit batch used up, give more
                    if (serverReceiver.getCredit() <= 0) {
                        serverReceiver.flow(creditBatch);
                    }
                });

                serverReceiver.open();

                serverReceiver.flow(creditBatch);
            });
        });
    }
}
