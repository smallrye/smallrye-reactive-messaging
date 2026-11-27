package io.smallrye.reactive.messaging.jms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.JMSException;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.DemandPauser;
import io.smallrye.reactive.messaging.jms.commit.JmsCommitHandler;
import io.smallrye.reactive.messaging.jms.fault.JmsFailureHandler;
import io.smallrye.reactive.messaging.providers.MetadataInjectableMessage;
import io.smallrye.reactive.messaging.providers.extension.PausableChannelDecorator.PauserChannel;

class JmsMessageTypeConverterTest {

    private final JmsMessageTypeConverter converter = new JmsMessageTypeConverter();

    private final AtomicInteger acks = new AtomicInteger();
    private final AtomicInteger nacks = new AtomicInteger();

    private jakarta.jms.Message jmsMessage;
    private IncomingJmsMessageMetadata jmsMetadata;
    private JmsSessionContext sessionContext;

    @BeforeEach
    void init() throws JMSException {
        jmsMessage = mock(jakarta.jms.Message.class);
        when(jmsMessage.getJMSMessageID()).thenReturn("ID:42");
        when(jmsMessage.getJMSCorrelationID()).thenReturn("correlation");
        when(jmsMessage.getStringProperty("custom")).thenReturn("value");
        // The body of the tracked original (mocks return null otherwise).
        when(jmsMessage.getBody(Object.class)).thenReturn("body");
        jmsMetadata = new IncomingJmsMessageMetadata(jmsMessage);
        sessionContext = new JmsSessionContext(null, JmsTransactionMode.LOCAL);
    }

    /**
     * A message as rebuilt by an interception: JMS metadata and acknowledgement kept, JMS type lost.
     */
    private <T> Message<T> intercepted(T payload) {
        return Message.of(payload, Metadata.of(jmsMetadata, sessionContext),
                () -> {
                    acks.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                failure -> {
                    nacks.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                });
    }

    /**
     * An incoming JMS message as created by the source, its commit and failure handlers counting.
     */
    private IncomingJmsMessage<Object> incoming() {
        JmsCommitHandler commitHandler = new JmsCommitHandler() {
            @Override
            public <T> Uni<Void> handle(IncomingJmsMessage<T> message, Metadata metadata) {
                acks.incrementAndGet();
                return Uni.createFrom().voidItem();
            }
        };
        JmsFailureHandler failureHandler = new JmsFailureHandler() {
            @Override
            public <T> Uni<Void> handle(IncomingJmsMessage<T> record, Throwable reason, Metadata metadata) {
                nacks.incrementAndGet();
                return Uni.createFrom().voidItem();
            }
        };
        IncomingJmsMessage<Object> message = new IncomingJmsMessage<>(jmsMessage, null, commitHandler, failureHandler);
        message.injectMetadata(sessionContext);
        return message;
    }

    @Test
    void canConvertOnlyIncomingJmsMessageTargetsOfInterceptedJmsMessages() {
        Message<String> message = intercepted("hello");

        assertThat(converter.canConvert(message, IncomingJmsMessage.class)).isTrue();
        assertThat(converter.canConvert(message, String.class)).isFalse();
        assertThat(converter.canConvert(message, Message.class)).isFalse();
        // Not a JMS message.
        assertThat(converter.canConvert(Message.of("hello"), IncomingJmsMessage.class)).isFalse();
        // Already a JMS message: nothing to restore.
        assertThat(converter.canConvert(incoming(), IncomingJmsMessage.class)).isFalse();
        Message<?> restored = converter.convert(message, IncomingJmsMessage.class);
        assertThat(converter.canConvert(restored, IncomingJmsMessage.class)).isFalse();
    }

    @Test
    void restoresAnIncomingJmsMessageAndDelegatesAcknowledgement() {
        Message<String> message = intercepted("hello");

        Message<?> converted = converter.convert(message, IncomingJmsMessage.class);

        assertThat(converted).isInstanceOf(IncomingJmsMessage.class);
        IncomingJmsMessage<?> restored = (IncomingJmsMessage<?>) converted;
        assertThat(restored.getPayload()).isEqualTo("hello");
        assertThat(restored.getMetadata()).isSameAs(message.getMetadata());
        assertThat(restored.getMetadata(JmsSessionContext.class)).containsSame(sessionContext);
        IncomingJmsMessageMetadata metadata = restored.getMetadata(IncomingJmsMessageMetadata.class).orElseThrow();
        assertThat(metadata.getMessageId()).isEqualTo("ID:42");
        assertThat(metadata.getCorrelationId()).isEqualTo("correlation");
        assertThat(metadata.getStringProperty("custom")).isEqualTo("value");
        assertThat(restored.unwrap(jakarta.jms.Message.class)).isSameAs(jmsMessage);
        assertThat(restored.unwrap(IncomingJmsMessageMetadata.class)).isSameAs(jmsMetadata);
        assertThat(restored.getContextMetadata()).isEmpty();

        restored.ack().toCompletableFuture().join();
        assertThat(acks).hasValue(1);
        restored.getAckWithMetadata().apply(Metadata.empty()).toCompletableFuture().join();
        assertThat(acks).hasValue(2);
        restored.nack(new Exception("boom")).toCompletableFuture().join();
        assertThat(nacks).hasValue(1);
    }

    @Test
    void restoresATrackedMessageAndKeepsTheTracking() {
        PauserChannel channel = new PauserChannel(new DemandPauser(), 1000, null, true);
        IncomingJmsMessage<Object> original = incoming();
        Message<?> tracked = channel.trackMessage(original);
        assertThat(tracked).isNotInstanceOf(IncomingJmsMessage.class);
        assertThat(channel.getInFlightCount()).isEqualTo(1);
        assertThat(converter.canConvert(tracked, IncomingJmsMessage.class)).isTrue();

        IncomingJmsMessage<?> restored = (IncomingJmsMessage<?>) converter.convert(tracked, IncomingJmsMessage.class);

        assertThat(restored.getPayload()).isEqualTo("body");
        assertThat(restored.unwrap(jakarta.jms.Message.class)).isSameAs(jmsMessage);
        assertThat(restored.getMetadata(JmsSessionContext.class)).containsSame(sessionContext);

        // Acknowledging the restored message acknowledges the tracked message, which commits through the original.
        restored.ack().toCompletableFuture().join();
        assertThat(acks).hasValue(1);
        assertThat(channel.getInFlightCount()).isZero();
    }

    @Test
    void injectsMetadataIntoTheInterceptedMessageToo() {
        IncomingJmsMessage<Object> original = incoming();
        IncomingJmsMessage<?> restored = new JmsMessageTypeConverter.RestoredIncomingJmsMessage<>(original);
        assertThat(restored).isNotSameAs(original);

        ((MetadataInjectableMessage<?>) restored).injectMetadata(new Injected());

        assertThat(restored.getMetadata(Injected.class)).isPresent();
        assertThat(original.getMetadata(Injected.class)).isPresent();
    }

    private static class Injected {
    }
}
