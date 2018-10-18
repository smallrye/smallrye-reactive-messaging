package io.smallrye.reactive.messaging.camel;

import org.apache.camel.Exchange;
import org.eclipse.microprofile.reactive.messaging.Message;

public class CamelMessage<T> implements Message<T> {
  private final Exchange exchange;

  public CamelMessage(Exchange exchange) {
    this.exchange = exchange;
  }

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
}
