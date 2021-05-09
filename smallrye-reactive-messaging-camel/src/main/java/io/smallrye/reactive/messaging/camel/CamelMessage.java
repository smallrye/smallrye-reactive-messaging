package io.smallrye.reactive.messaging.camel;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.apache.camel.Exchange;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

public class CamelMessage<T> implements Message<T> {
    private final Exchange exchange;
    private final Metadata metadata;
    private final CamelFailureHandler onNack;

    public CamelMessage(Exchange exchange, CamelFailureHandler onNack) {
        this.exchange = exchange;
        this.onNack = onNack;
        metadata = Metadata.of(new IncomingExchangeMetadata(exchange));
    }

    @SuppressWarnings("unchecked")
    @Override
    public T getPayload() {
        return (T) exchange.getIn().getBody();
    }

    public T getPayload(Class<T> clazz) {
        return exchange.getIn().getBody(clazz);
    }

    public Exchange getExchange() {
        return exchange;
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
        return onNack.handle(this, reason);
    }

    @Override
    public Function<Throwable, CompletionStage<Void>> getNack() {
        return this::nack;
    }
}
