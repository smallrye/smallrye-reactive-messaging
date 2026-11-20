package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption.RetainedHandlingPolicy;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSession;
import io.smallrye.reactive.messaging.mqtt.session.RequestedQoS;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.mqtt.messages.MqttPublishMessage;

class MqttSourceStartupTest {

    private static final int MESSAGE_COUNT = 5000;
    private final Vertx vertx = Vertx.vertx();

    @AfterEach
    void close() {
        vertx.closeAndAwait();
    }

    @Test
    void retainsMessagesDeliveredImmediatelyOnSubscription() {
        assertRetainedReplay(false, false, false);
    }

    @Test
    void retainsMessagesWithV5SubscriptionOptions() {
        assertRetainedReplay(true, false, false);
    }

    @Test
    void startsOnlyOnceForBroadcastSubscribers() {
        assertRetainedReplay(false, true, false);
    }

    @Test
    void retainsMessagesWhenAnotherChannelAlreadyUsesTheClient() {
        assertRetainedReplay(false, false, true);
    }

    private void assertRetainedReplay(boolean v5Options, boolean broadcast, boolean sharedClient) {
        String topic = "retained/startup";
        var messages = BroadcastProcessor.<MqttPublishMessage> create();
        var holder = mock(Clients.ClientHolder.class);
        var client = mock(MqttClientSession.class);
        when(holder.retain(any())).thenReturn(holder);
        when(holder.stream()).thenReturn(messages);
        when(holder.getClient()).thenReturn(client);
        when(holder.start()).thenReturn(Future.succeededFuture());

        var replay = (org.mockito.stubbing.Answer<Future<Integer>>) invocation -> {
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                var message = io.vertx.mqtt.messages.MqttPublishMessage.create(i + 1, MqttQoS.AT_LEAST_ONCE,
                        false, true, topic, Buffer.buffer(Integer.toString(i)));
                messages.onNext(MqttPublishMessage.newInstance(message));
            }
            return Future.succeededFuture(1);
        };
        if (v5Options) {
            when(client.subscribe(topic, RequestedQoS.QOS_1, true, true,
                    RetainedHandlingPolicy.valueOf(1), 7)).thenAnswer(replay);
        } else {
            when(client.subscribe(topic, RequestedQoS.QOS_1)).thenAnswer(replay);
        }

        when(client.subscribe("heartbeat", RequestedQoS.QOS_0)).thenReturn(Future.succeededFuture(0));

        try (MockedStatic<Clients> clients = mockStatic(Clients.class)) {
            clients.when(() -> Clients.getHolder(eq(vertx), any(), isNull())).thenReturn(holder);
            Cancellable heartbeat = null;
            if (sharedClient) {
                var heartbeatConfig = new MapBasedConfig().with("channel-name", "heartbeat")
                        .with("host", "localhost").with("topic", "heartbeat");
                var heartbeatSource = new MqttSource(vertx, new MqttConnectorIncomingConfiguration(heartbeatConfig), null,
                        null);
                heartbeat = Multi.createFrom().publisher(heartbeatSource.getSource()).subscribe().with(message -> {
                });
                clearInvocations(holder, client);
            }
            var config = new MapBasedConfig()
                    .with("channel-name", "startup")
                    .with("host", "localhost")
                    .with("topic", topic)
                    .with("qos", 1)
                    .with("buffer-size", MESSAGE_COUNT)
                    .with("broadcast", broadcast);
            if (v5Options) {
                config.with("mqtt-version", 5).with("no-local", true).with("retain-as-published", true)
                        .with("retain-handling", 1).with("subscription-identifier", 7);
            }
            var source = new MqttSource(vertx, new MqttConnectorIncomingConfiguration(config), null, null);
            // Connection readiness remains available before the consumer attaches.
            verify(holder).start();
            verifyNoInteractions(client);

            List<Integer> received = new CopyOnWriteArrayList<>();
            var subscription = Multi.createFrom().publisher(source.getSource()).subscribe().with(
                    message -> received.add(Integer.parseInt(new String(message.getPayload(), StandardCharsets.UTF_8))));
            try {
                await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(received).hasSize(MESSAGE_COUNT));
                assertThat(received).containsExactlyElementsOf(IntStream.range(0, MESSAGE_COUNT).boxed().toList());
                if (broadcast) {
                    var second = Multi.createFrom().publisher(source.getSource()).subscribe().with(message -> {
                    });
                    second.cancel();
                }
                verify(holder).start();
                if (v5Options) {
                    verify(client).subscribe(topic, RequestedQoS.QOS_1, true, true,
                            RetainedHandlingPolicy.valueOf(1), 7);
                } else {
                    verify(client).subscribe(topic, RequestedQoS.QOS_1);
                }
            } finally {
                subscription.cancel();
                if (heartbeat != null) {
                    heartbeat.cancel();
                }
            }
        }
    }
}
