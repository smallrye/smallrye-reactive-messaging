package io.smallrye.reactive.messaging.impl;

import io.smallrye.reactive.messaging.StreamFactory;
import io.smallrye.reactive.messaging.spi.ConnectorLiteral;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import static io.smallrye.reactive.messaging.impl.ConnectorConfig.EMPTY_CONFIG;

@ApplicationScoped
public class StreamFactoryImpl implements StreamFactory {

  private final Instance<IncomingConnectorFactory> incomingConnectorFactories;
  private final Instance<OutgoingConnectorFactory> outgoingConnectorFactories;


  public StreamFactoryImpl() {
    // Default constructor is required - https://github.com/smallrye/smallrye-reactive-messaging/issues/97
    this.incomingConnectorFactories = null;
    this.outgoingConnectorFactories = null;
  }

  @Inject
  public StreamFactoryImpl(@Any Instance<IncomingConnectorFactory> pubs,
                           @Any Instance<OutgoingConnectorFactory> subs) {
    this.incomingConnectorFactories = pubs;
    this.outgoingConnectorFactories = subs;
  }

  @Override
  public synchronized PublisherBuilder<? extends Message> createPublisherBuilder(String connector, Config config) {
    if (incomingConnectorFactories == null) {
      throw onUnknownConnector(connector);
    }
    if (connector == null) {
      throw new IllegalArgumentException("`connector` must not be null");
    }
    IncomingConnectorFactory factory = incomingConnectorFactories
      .select(IncomingConnectorFactory.class, ConnectorLiteral.of(connector))
      .stream()
      .findFirst()
      .orElseThrow(() -> onUnknownConnector(connector));

    if (config == null) {
      config = EMPTY_CONFIG;
    }
    return factory.getPublisherBuilder(config);
  }

  private static IllegalArgumentException onUnknownConnector(String connector) {
    return new IllegalArgumentException("Unknown connector: " + connector);
  }

  @Override
  public SubscriberBuilder<? extends Message, Void> createSubscriberBuilder(String connector, Config config) {
    if (outgoingConnectorFactories == null) {
      throw onUnknownConnector(connector);
    }
    if (connector == null) {
      throw new IllegalArgumentException("`connector` must not be null");
    }
    OutgoingConnectorFactory factory = outgoingConnectorFactories
      .select(OutgoingConnectorFactory.class, ConnectorLiteral.of(connector))
      .stream()
      .findFirst()
      .orElseThrow(() -> onUnknownConnector(connector));

    if (config == null) {
      config = EMPTY_CONFIG;
    }
    return factory.getSubscriberBuilder(config);
  }
}
