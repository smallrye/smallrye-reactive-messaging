package io.smallrye.reactive.messaging.camel;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.subscription.DemandPauser;
import io.smallrye.reactive.messaging.providers.extension.PausableChannelDecorator.PauserChannel;

class CamelMessageTypeConverterTest {

    private final CamelMessageTypeConverter converter = new CamelMessageTypeConverter();

    private final AtomicInteger acks = new AtomicInteger();
    private final AtomicInteger nacks = new AtomicInteger();

    private CamelContext context;
    private Exchange exchange;

    @BeforeEach
    void init() {
        context = new DefaultCamelContext();
        exchange = new DefaultExchange(context);
        exchange.getIn().setBody("body");
        exchange.getIn().setHeader("custom", "value");
    }

    @AfterEach
    void close() {
        context.stop();
    }

    /**
     * A message as rebuilt by an interception: exchange metadata and acknowledgement kept, Camel type lost.
     */
    private <T> Message<T> intercepted(T payload) {
        return Message.of(payload, Metadata.of(new IncomingExchangeMetadata(exchange)),
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
     * An incoming Camel message as created by the source, its failure handler counting.
     */
    private CamelMessage<Object> incoming() {
        return new CamelMessage<>(exchange, (message, reason) -> {
            nacks.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });
    }

    @Test
    void canConvertOnlyCamelMessageTargetsOfInterceptedCamelMessages() {
        Message<String> message = intercepted("hello");

        assertThat(converter.canConvert(message, CamelMessage.class)).isTrue();
        assertThat(converter.canConvert(message, String.class)).isFalse();
        assertThat(converter.canConvert(message, Message.class)).isFalse();
        // Not a Camel message.
        assertThat(converter.canConvert(Message.of("hello"), CamelMessage.class)).isFalse();
        // Already a Camel message: nothing to restore.
        assertThat(converter.canConvert(incoming(), CamelMessage.class)).isFalse();
        Message<?> restored = converter.convert(message, CamelMessage.class);
        assertThat(converter.canConvert(restored, CamelMessage.class)).isFalse();
    }

    @Test
    void restoresACamelMessageAndDelegatesAcknowledgement() {
        Message<String> message = intercepted("hello");

        Message<?> converted = converter.convert(message, CamelMessage.class);

        assertThat(converted).isInstanceOf(CamelMessage.class);
        CamelMessage<?> restored = (CamelMessage<?>) converted;
        assertThat(restored.getPayload()).isEqualTo("hello");
        assertThat(restored.getMetadata()).isSameAs(message.getMetadata());
        assertThat(restored.getExchange()).isSameAs(exchange);
        assertThat(restored.getExchange().getIn().getHeader("custom")).isEqualTo("value");

        restored.ack().toCompletableFuture().join();
        assertThat(acks).hasValue(1);
        restored.getAckWithMetadata().apply(Metadata.empty()).toCompletableFuture().join();
        assertThat(acks).hasValue(2);
        restored.nack(new Exception("boom")).toCompletableFuture().join();
        assertThat(nacks).hasValue(1);
        restored.getNackWithMetadata().apply(new Exception("boom"), Metadata.empty()).toCompletableFuture().join();
        assertThat(nacks).hasValue(2);
    }

    @Test
    void restoresATrackedMessageAndKeepsTheTracking() {
        PauserChannel channel = new PauserChannel(new DemandPauser(), 1000, null, true);
        Message<?> tracked = channel.trackMessage(incoming());
        assertThat(tracked).isNotInstanceOf(CamelMessage.class);
        assertThat(channel.getInFlightCount()).isEqualTo(1);
        assertThat(converter.canConvert(tracked, CamelMessage.class)).isTrue();

        CamelMessage<?> restored = (CamelMessage<?>) converter.convert(tracked, CamelMessage.class);

        assertThat(restored.getPayload()).isEqualTo("body");
        assertThat(restored.getExchange()).isSameAs(exchange);

        // Nacking the restored message nacks the tracked message, which applies the failure strategy of the original.
        restored.nack(new Exception("boom")).toCompletableFuture().join();
        assertThat(nacks).hasValue(1);
        assertThat(channel.getInFlightCount()).isZero();
    }
}
