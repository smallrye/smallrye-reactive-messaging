package io.smallrye.reactive.messaging.impl;

import io.smallrye.reactive.messaging.StreamFactory;
import io.smallrye.reactive.messaging.StreamRegistry;
import io.smallrye.reactive.messaging.spi.IncomingConnectorFactory;
import io.smallrye.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.smallrye.reactive.messaging.impl.ConnectorConfig.EMPTY_CONFIG;

@ApplicationScoped
public class StreamFactoryImpl implements StreamFactory {

  private static final String NAME_MUST_BE_SET = "'name' must be set";

  private final StreamRegistry registry;

  private final Map<String, IncomingConnectorFactory> publisherFactories = new HashMap<>();
  private Map<String, OutgoingConnectorFactory> subscriberFactories = new HashMap<>();


  @Inject
  public StreamFactoryImpl(@Any Instance<IncomingConnectorFactory> pubs,
                           @Any Instance<OutgoingConnectorFactory> subs,
                           StreamRegistry registry) {
    this.registry = registry;
    pubs.stream().forEach(pf -> publisherFactories.put(pf.type().getName(), pf));
    subs.stream().forEach(pf -> subscriberFactories.put(pf.type().getName(), pf));
  }

  @Override
  public synchronized PublisherBuilder<? extends Message> createPublisherBuilderAndRegister(String name, Config config) {
    Objects.requireNonNull(name, NAME_MUST_BE_SET);
    String type = config.getOptionalValue("type", String.class)
      .orElseThrow(() -> new IllegalArgumentException("Invalid publisher, no type for " + name));
    PublisherBuilder<? extends Message> builder = createPublisherBuilder(type, config);
    this.registry.register(name, builder);
    return builder;
  }

  @Override
  public synchronized SubscriberBuilder<? extends Message, Void> createSubscriberBuilderAndRegister(String name, Config config) {
    Objects.requireNonNull(name, NAME_MUST_BE_SET);
    String type = config.getOptionalValue("type", String.class)
      .orElseThrow(() -> new IllegalArgumentException("Invalid subscriber, no type for " + name));
    SubscriberBuilder<? extends Message, Void> builder = createSubscriberBuilder(type, config);
    this.registry.register(name, builder);
    return builder;
  }

  @Override
  public synchronized PublisherBuilder<? extends Message> createPublisherBuilder(String type, Config config) {
    IncomingConnectorFactory factory = publisherFactories.get(
      Objects.requireNonNull(type, "'type' must be set, known types are: " + publisherFactories.keySet()));
    if (factory == null) {
      throw new IllegalArgumentException("Unknown type: " + type + ", known types are: " + publisherFactories.keySet());
    }
    if (config == null) {
      config = EMPTY_CONFIG;
    }
    return factory.getPublisherBuilder(config);
  }

  @Override
  public SubscriberBuilder<? extends Message, Void> createSubscriberBuilder(String type, Config config) {
    OutgoingConnectorFactory factory = subscriberFactories.get(
      Objects.requireNonNull(type, "'type' must be set, known types are: " + subscriberFactories.keySet()));
    if (factory == null) {
      throw new IllegalArgumentException("Unknown type: " + type + ", known types are: " + subscriberFactories.keySet());
    }
    if (config == null) {
      config = EMPTY_CONFIG;
    }
    return factory.getSubscriberBuilder(config);
  }
}
