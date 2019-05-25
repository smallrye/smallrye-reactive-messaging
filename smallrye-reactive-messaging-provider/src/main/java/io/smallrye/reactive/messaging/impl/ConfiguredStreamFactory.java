package io.smallrye.reactive.messaging.impl;

import io.smallrye.reactive.messaging.StreamRegistar;
import io.smallrye.reactive.messaging.StreamRegistry;
import io.smallrye.reactive.messaging.spi.ConnectorLiteral;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.BeanAttributes;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.DeploymentException;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Look for stream factories and get instances.
 */
@ApplicationScoped
public class ConfiguredStreamFactory implements StreamRegistar {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfiguredStreamFactory.class);
  private static final String SOURCE_CONFIG_PREFIX = "mp.messaging.incoming";
  private static final String SINK_CONFIG_PREFIX = "mp.messaging.outgoing";


  private final Instance<IncomingConnectorFactory> incomingConnectorFactories;
  private final Instance<OutgoingConnectorFactory> outgoingConnectorFactories;

  protected final Config config;
  protected final StreamRegistry registry;

  // CDI requirement for normal scoped beans
  ConfiguredStreamFactory() {
    this.incomingConnectorFactories = null;
    this.outgoingConnectorFactories = null;
    this.config = null;
    this.registry = null;
  }

  @Inject
  public ConfiguredStreamFactory(@Any Instance<IncomingConnectorFactory> incomingConnectorFactories,
                                 @Any Instance<OutgoingConnectorFactory> outgoingConnectorFactories,
                                 Instance<Config> config, @Any Instance<StreamRegistry> registry,
                                 BeanManager beanManager) {

    this.registry = registry.get();
    if (config.isUnsatisfied()) {
      this.incomingConnectorFactories = null;
      this.outgoingConnectorFactories = null;
      this.config = null;
    } else {
      this.incomingConnectorFactories = incomingConnectorFactories;
      this.outgoingConnectorFactories = outgoingConnectorFactories;
      List<String> incomingConnectors = getConnectors(beanManager, IncomingConnectorFactory.class);
      List<String> outgoingConnectors = getConnectors(beanManager, OutgoingConnectorFactory.class);
      LOGGER.info("Found incoming connectors: {}", incomingConnectors);
      LOGGER.info("Found outgoing connectors: {}", outgoingConnectors);
      //TODO Should we try to merge all the config?
      // For now take the first one.
      this.config = config.stream().findFirst()
        .orElseThrow(() -> new IllegalStateException("Unable to retrieve the config"));
    }
  }

  private List<String> getConnectors(BeanManager beanManager, Class<?> clazz) {
    return beanManager.getBeans(clazz).stream()
      .map(BeanAttributes::getQualifiers)
      .flatMap(set -> set.stream().filter(a -> a.annotationType().equals(Connector.class)))
      .map(annotation -> ((Connector) annotation).value())
      .collect(Collectors.toList());
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

    register(sourceConfiguration, sinkConfiguration);
  }

  void register(Map<String, ConnectorConfig> sourceConfiguration, Map<String, ConnectorConfig> sinkConfiguration) {
    try {
      sourceConfiguration.forEach((name, conf) -> registry.register(name, createPublisherBuilder(name, conf)));
      sinkConfiguration.forEach((name, conf) -> registry.register(name, createSubscriberBuilder(name, conf)));
    } catch (RuntimeException e) {
      LOGGER.error("Unable to create the publisher or subscriber during initialization", e);
      throw e;
    }
  }

  private static Optional<String> getConnectorAttribute(Config config) {
    Optional<String> optional = config.getOptionalValue("connector", String.class);
    if (optional.isPresent()) {
      return optional;
    } else {
      return config.getOptionalValue("type", String.class);
    }
  }

  private PublisherBuilder<? extends Message> createPublisherBuilder(String name, Config config) {
    // Extract the type and throw an exception if missing
    String connector = getConnectorAttribute(config)
      .orElseThrow(() -> new IllegalArgumentException("Invalid incoming configuration, " +
        "no connector attribute for " + name));

    // Look for the factory and throw an exception if missing
    IncomingConnectorFactory mySourceFactory = incomingConnectorFactories.select(ConnectorLiteral.of(connector))
      .stream().findFirst().orElseThrow(() -> new IllegalArgumentException("Unknown connector for " + name + "."));

    return mySourceFactory.getPublisherBuilder(config);
  }

  private SubscriberBuilder<? extends Message, Void> createSubscriberBuilder(String name, Config config) {
    // Extract the type and throw an exception if missing
    String connector = getConnectorAttribute(config)
      .orElseThrow(() -> new IllegalArgumentException("Invalid outgoing configuration, " +
        "no connector attribute for " + name));

    // Look for the factory and throw an exception if missing
    OutgoingConnectorFactory mySinkFactory = outgoingConnectorFactories.select(ConnectorLiteral.of(connector))
      .stream().findFirst().orElseThrow(() -> new IllegalArgumentException("Unknown connector for " + name + "."));

    return mySinkFactory.getSubscriberBuilder(config);
  }
}
