package io.smallrye.reactive.messaging.impl;

import io.smallrye.reactive.messaging.StreamRegistar;
import io.smallrye.reactive.messaging.StreamRegistry;
import io.smallrye.reactive.messaging.spi.IncomingConnectorFactory;
import io.smallrye.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Look for stream factories and get instances.
 */
@ApplicationScoped
public class ConfiguredStreamFactory implements StreamRegistar {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfiguredStreamFactory.class);
  private static final String SOURCE_CONFIG_PREFIX = "mp.messaging.provider.incoming";
  private static final String SINK_CONFIG_PREFIX = "mp.messaging.provider.outgoing";


  private final List<IncomingConnectorFactory> sourceFactories;
  private final List<OutgoingConnectorFactory> sinkFactories;
  private final Config config;
  private final StreamRegistry registry;

  private final Map<String, PublisherBuilder<? extends Message>> sources = new HashMap<>();
  private final Map<String, SubscriberBuilder<? extends Message, Void>> sinks = new HashMap<>();

  // CDI requirement for normal scoped beans
  ConfiguredStreamFactory() {
    this.sourceFactories = null;
    this.sinkFactories = null;
    this.config = null;
    this.registry = null;
  }

  @Inject
  public ConfiguredStreamFactory(@Any Instance<IncomingConnectorFactory> sourceFactories,
                                 @Any Instance<OutgoingConnectorFactory> sinkFactories,
                                 Instance<Config> config, @Any Instance<StreamRegistry> registry) {

    this.registry = registry.get();
    if (config.isUnsatisfied()) {
      this.sourceFactories = Collections.emptyList();
      this.sinkFactories = Collections.emptyList();
      this.config = null;
    } else {
      this.sourceFactories = sourceFactories.stream().collect(Collectors.toList());
      this.sinkFactories = sinkFactories.stream().collect(Collectors.toList());
      LOGGER.info("Found incoming connectors: {}", sourceFactories.stream().map(IncomingConnectorFactory::type).collect(Collectors.toList()));
      LOGGER.info("Found outgoing connectors: {}", sinkFactories.stream().map(OutgoingConnectorFactory::type).collect(Collectors.toList()));
      //TODO Should we try to merge all the config?
      // For now take the first one.
      this.config = config.stream().findFirst()
        .orElseThrow(() -> new IllegalStateException("Unable to retrieve the config"));
    }
  }

  static Map<String, ConnectorConfig> extractConfigurationFor(String prefix, Config root) {
    Iterable<String> names = root.getPropertyNames();
    Map<String, ConnectorConfig> configs = new HashMap<>();
    names.forEach(key -> {
      // $prefix.$name.key=value
      if (key.startsWith(prefix)) {
        // Extract the name
        String name = key.substring(prefix.length() + 1);
        if (name.contains(".")) { // We must remove the part after the first dot
          String tmp = name;
          name = tmp.substring(0, tmp.indexOf('.'));
          configs.put(name, new ConnectorConfig(prefix + "." + name, root, name));
        } else {
          configs.put(name, new ConnectorConfig(prefix + "." + name, root, name));
        }
      }
    });
    return configs;
  }

  @Override
  public void initialize() {
    if (this.config == null) {
      LOGGER.info("No MicroProfile Config found, skipping");
      return;
    }

    LOGGER.info("Stream manager initializing...");

    Map<String, ConnectorConfig> sourceConfiguration = extractConfigurationFor(SOURCE_CONFIG_PREFIX, config);
    Map<String, ConnectorConfig> sinkConfiguration = extractConfigurationFor(SINK_CONFIG_PREFIX, config);

    try {
      sourceConfiguration.forEach((name, conf) -> registry.register(name, createPublisherBuilder(name, conf)));
      sinkConfiguration.forEach((name, conf) -> registry.register(name, createSubscriberBuilder(name, conf)));
    } catch (Exception e) {
      LOGGER.error("Unable to create the publisher or subscriber during initialization", e);
    }
  }

  private PublisherBuilder<? extends Message> createPublisherBuilder(String name, Config config) {
    // Extract the type and throw an exception if missing
    String type = config.getOptionalValue("type", String.class)
      .orElseThrow(() -> new IllegalArgumentException("Invalid source, no type for " + name));

    // Look for the factory and throw an exception if missing
    IncomingConnectorFactory mySourceFactory = sourceFactories.stream().filter(factory -> factory.type().getName().equalsIgnoreCase(type)).findFirst()
      .orElseThrow(() -> new IllegalArgumentException("Unknown source type for " + name + ", supported types are "
        + sourceFactories.stream().map(sf -> sf.type().getName()).collect(Collectors.toList()))
      );

    return mySourceFactory.getPublisherBuilder(config);
  }


  private SubscriberBuilder<? extends Message, Void> createSubscriberBuilder(String name, Config config) {
    // Extract the type and throw an exception if missing
    String type = config.getOptionalValue("type", String.class)
      .orElseThrow(() -> new IllegalArgumentException("Invalid sink, no type for " + name));

    // Look for the factory and throw an exception if missing
    OutgoingConnectorFactory mySinkFactory = sinkFactories.stream().filter(factory -> factory.type().getName().equalsIgnoreCase(type)).findFirst()
      .orElseThrow(() -> new IllegalArgumentException("Unknown sink type for " + name + ", supported types are "
        + sinkFactories.stream().map(sf -> sf.type().getName()).collect(Collectors.toList()))
      );

    return mySinkFactory.getSubscriberBuilder(config);
  }
}
