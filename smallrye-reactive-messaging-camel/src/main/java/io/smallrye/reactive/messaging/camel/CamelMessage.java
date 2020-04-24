package io.smallrye.reactive.messaging.camel;

import org.apache.camel.Exchange;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

public class CamelMessage<T> implements Message<T> {
    private final Exchange exchange;
    private final Metadata metadata;

    public CamelMessage(Exchange exchange) {
        this.exchange = exchange;
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
}
