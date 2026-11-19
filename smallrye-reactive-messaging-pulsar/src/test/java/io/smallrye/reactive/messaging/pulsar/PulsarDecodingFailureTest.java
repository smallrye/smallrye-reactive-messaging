package io.smallrye.reactive.messaging.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.pulsar.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.pulsar.fault.PulsarFailStop;
import io.smallrye.reactive.messaging.pulsar.fault.PulsarIgnore;
import io.smallrye.reactive.messaging.pulsar.fault.PulsarNack;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

class PulsarDecodingFailureTest {

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void ignoresMalformedMessageAndDeliversCachedPayload(boolean batch) throws Exception {
        try (Fixture f = new Fixture(batch, new PulsarIgnore.Factory())) {
            Message<String> invalid = invalid();
            Message<String> valid = valid();
            f.receive(invalid, valid);
            var incoming = Multi.createFrom().publisher(f.channel.getPublisher()).toUni().await()
                    .atMost(Duration.ofSeconds(5));
            if (batch) {
                var message = (PulsarIncomingBatchMessage<?>) incoming;
                assertThat(message.getPayload()).isEqualTo(List.of("valid"));
                assertThat(message.unwrap()).hasSize(1);
                assertThat(message.unwrap().iterator().next()).isSameAs(valid);
                assertThat(message.getMessages()).hasSize(1);
            } else {
                assertThat(incoming.getPayload()).isEqualTo("valid");
                assertThat(incoming.getPayload()).isEqualTo("valid");
            }
            verify(invalid).release();
            verify(f.consumer).acknowledgeAsync(MessageId.earliest);
            verify(valid, times(1)).getValue();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void nacksMalformedMessageAndContinues(boolean batch) throws Exception {
        try (Fixture f = new Fixture(batch, new PulsarNack.Factory())) {
            f.receive(invalid(), valid());
            var incoming = Multi.createFrom().publisher(f.channel.getPublisher()).toUni().await()
                    .atMost(Duration.ofSeconds(5));
            assertThat(incoming).isNotNull();
            verify(f.consumer).negativeAcknowledge(MessageId.earliest);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void failStrategyPropagatesDecodingFailure(boolean batch) throws Exception {
        try (Fixture f = new Fixture(batch, new PulsarFailStop.Factory())) {
            f.receive(invalid(), valid());
            assertThatThrownBy(() -> Multi.createFrom().publisher(f.channel.getPublisher()).toUni().await()
                    .atMost(Duration.ofSeconds(5)))
                    .isInstanceOf(SchemaSerializationException.class);
            verify(f.consumer).close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void failureHandlerErrorsArePropagated(boolean batch) throws Exception {
        IllegalStateException failure = new IllegalStateException("ack failed");
        try (Fixture f = new Fixture(batch, (consumer, config, report) -> (message, reason, metadata) -> Uni
                .createFrom().failure(failure))) {
            f.receive(invalid(), valid());
            assertThatThrownBy(() -> Multi.createFrom().publisher(f.channel.getPublisher()).toUni().await()
                    .atMost(Duration.ofSeconds(5))).isSameAs(failure);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void skipsAllInvalidBatchOrMessages(boolean batch) throws Exception {
        try (Fixture f = new Fixture(batch, new PulsarIgnore.Factory())) {
            Message<String> valid = valid();
            Message<String> invalid = invalid();
            if (batch) {
                when(f.consumer.batchReceiveAsync()).thenReturn(
                        CompletableFuture.completedFuture(messages(invalid)),
                        CompletableFuture.completedFuture(messages(valid)), new CompletableFuture<>());
            } else {
                f.receive(invalid, valid);
            }
            var incoming = Multi.createFrom().publisher(f.channel.getPublisher()).toUni().await()
                    .atMost(Duration.ofSeconds(5));
            assertThat(incoming.getPayload()).isEqualTo(batch ? List.of("valid") : "valid");
        }
    }

    private static Message<String> invalid() {
        Message<String> message = mock(Message.class);
        when(message.getMessageId()).thenReturn(MessageId.earliest);
        when(message.getValue()).thenThrow(new SchemaSerializationException("invalid JSON"));
        return message;
    }

    private static Message<String> valid() {
        Message<String> message = mock(Message.class);
        when(message.getValue()).thenReturn("valid");
        return message;
    }

    @SafeVarargs
    private static Messages<String> messages(Message<String>... messages) {
        List<Message<String>> list = List.of(messages);
        return new Messages<>() {
            @Override
            public int size() {
                return list.size();
            }

            @Override
            public Iterator<Message<String>> iterator() {
                return list.iterator();
            }
        };
    }

    private static class Fixture implements AutoCloseable {
        final Vertx vertx = Vertx.vertx();
        final Consumer<String> consumer = mock(Consumer.class);
        final PulsarIncomingChannel<String> channel;
        final boolean batch;

        Fixture(boolean batch, PulsarFailureHandler.Factory failureFactory) throws Exception {
            this.batch = batch;
            PulsarClient client = mock(PulsarClient.class);
            ConsumerBuilder<String> builder = mock(ConsumerBuilder.class);
            when(client.newConsumer(Schema.STRING)).thenReturn(builder);
            when(builder.subscribe()).thenReturn(consumer);
            when(consumer.acknowledgeAsync(any(MessageId.class))).thenReturn(CompletableFuture.completedFuture(null));
            ConfigResolver resolver = mock(ConfigResolver.class);
            when(resolver.getConsumerConf(any())).thenReturn(new ConsumerConfigurationData<>());
            when(resolver.configure(any(ConsumerBuilder.class), any(), any())).thenReturn(builder);
            var config = new PulsarConnectorIncomingConfiguration(new MapBasedConfig()
                    .with("channel-name", "test").with("batchReceive", batch).with("tracing-enabled", false));
            channel = new PulsarIncomingChannel<>(client, vertx, Schema.STRING,
                    (c, ic) -> message -> Uni.createFrom().completionStage(
                            () -> c.acknowledgeAsync(message.getMessageId()))
                            .invoke(() -> message.unwrap().release()),
                    failureFactory, config, resolver, UnsatisfiedInstance.instance());
        }

        void receive(Message<String> invalid, Message<String> valid) {
            if (batch) {
                when(consumer.batchReceiveAsync()).thenReturn(
                        CompletableFuture.completedFuture(messages(invalid, valid)), new CompletableFuture<>());
            } else {
                when(consumer.receiveAsync()).thenReturn(CompletableFuture.completedFuture(invalid),
                        CompletableFuture.completedFuture(valid), new CompletableFuture<>());
            }
        }

        @Override
        public void close() {
            channel.close();
            vertx.closeAndAwait();
        }
    }
}
