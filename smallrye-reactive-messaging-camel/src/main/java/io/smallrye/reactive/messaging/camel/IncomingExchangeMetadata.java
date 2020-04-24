package io.smallrye.reactive.messaging.camel;

import org.apache.camel.Exchange;

public class IncomingExchangeMetadata {

    private final Exchange exchange;

    public IncomingExchangeMetadata(Exchange exchange) {
        this.exchange = exchange;
    }

    public Exchange getExchange() {
        return exchange;
    }

}
