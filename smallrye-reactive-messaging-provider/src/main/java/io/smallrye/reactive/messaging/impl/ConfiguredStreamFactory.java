package io.smallrye.reactive.messaging.impl;

import io.smallrye.reactive.messaging.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.StreamRegistar;
import io.smallrye.reactive.messaging.StreamRegistry;
import io.smallrye.reactive.messaging.spi.PublisherFactory;
import io.smallrye.reactive.messaging.spi.SubscriberFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

/**
 * Look for stream factories and createPublisher instances.
 */
@ApplicationScoped
public class ConfiguredStreamFactory implements StreamRegistar {

  private static final Logger LOGGER = LogManager.getLogger(ConfiguredStreamFactory.class);
  public static final String SOURCE_CONFIG_PREFIX = "smallrye.messaging.source";
  public static final String SINK_CONFIG_PREFIX = "smallrye.messaging.sink";


  private final List<PublisherFactory> sourceFactories;
  private final List<SubscriberFactory> sinkFactories;
  private final Config config;
  private final StreamRegistry registry;

  private final Map<String, Publisher<? extends Message>> sources = new HashMap<>();
  private final Map<String, Subscriber<? extends Message>> sinks = new HashMap<>();

  @Inject
  public ConfiguredStreamFactory(@Any Instance<PublisherFactory> sourceFactories, @Any Instance<SubscriberFactory> sinkFactories,
                                 Instance<Config> config, @Any Instance<StreamRegistry> registry) {

    this.registry = registry.get();
    if (config.isUnsatisfied()) {
      this.sourceFactories = Collections.emptyList();
      this.sinkFactories = Collections.emptyList();
      this.config = null;
    } else {
      this.sourceFactories = sourceFactories.stream().collect(Collectors.toList());
      this.sinkFactories = sinkFactories.stream().collect(Collectors.toList());
      LOGGER.info("Found source types: " + sourceFactories.stream().map(PublisherFactory::type).collect(Collectors.toList()));
      LOGGER.info("Found sink types: " + sinkFactories.stream().map(SubscriberFactory::type).collect(Collectors.toList()));
      //TODO Should we try to merge all the config?
      // For now take the first one.
      this.config = config.stream().findFirst()
        .orElseThrow(() -> new IllegalStateException("Unable to retrieve the config"));
    }
  }

  static Map<String, Map<String, String>> extractConfigurationFor(String prefix, Config root) {
    Iterable<String> names = root.getPropertyNames();
    Map<String, Map<String, String>> configs = new HashMap<>();
    names.forEach(key -> {
      // $prefix.name.key=value
      if (key.startsWith(prefix)) {
        // Extract the name
        String name = key.substring(prefix.length() +1);
        if (name.contains(".")) {
          String tmp = name;
          name = tmp.substring(0, tmp.indexOf('.'));
          String subkey = tmp.substring(tmp.indexOf('.') + 1);
          Map<String, String> map = configs.computeIfAbsent(name, x -> new HashMap<>());
          map.put(subkey, root.getValue(key, String.class));
        } else {
          Map<String, String> map = configs.computeIfAbsent(name, x -> new HashMap<>());
          map.put("name", root.getValue(key, String.class));
        }
      }
    });
    return configs;
  }

  @Override
  public CompletionStage<Void> initialize() {
    if (this.config == null) {
      LOGGER.info("No MicroProfile Config found, skipping");
      return CompletableFuture.completedFuture(null);
    }

    LOGGER.info("Stream manager initializing...");

    Map<String, Map<String, String>> sourceConfiguration = extractConfigurationFor(SOURCE_CONFIG_PREFIX, config);
    Map<String, Map<String, String>> sinkConfiguration = extractConfigurationFor(SINK_CONFIG_PREFIX, config);

    List<CompletionStage> tasks = new ArrayList<>();
    sourceConfiguration.forEach((name, conf) -> tasks.add(createSourceFromConfig(name, conf)));
    sinkConfiguration.forEach((name, conf) -> tasks.add(createSinkFromConfig(name, conf)));

    CompletableFuture<Void> all = CompletableFuture.allOf(tasks.stream().map(CompletionStage::toCompletableFuture)
      .toArray((IntFunction<CompletableFuture<?>[]>) CompletableFuture[]::new));
    return all.whenComplete((x, err) -> {
      if (err == null) {
        LOGGER.info("Publishers created during initialization: {}", sources.keySet());
        LOGGER.info("Subscribers created during initialization: {}", sinks.keySet());

        sources.forEach(registry::register);
        sinks.forEach(registry::register);
      }
    });
  }

  private CompletionStage createSourceFromConfig(String name, Map<String, String> conf) {
    // Extract the type and throw an exception if missing
    String type = Optional.ofNullable(conf.get("type")).map(Object::toString)
      .orElseThrow(() -> new IllegalArgumentException("Invalid source, no type for " + name));

    // Look for the factory and throw an exception if missing
    PublisherFactory mySourceFactory = sourceFactories.stream().filter(factory -> factory.type().getName().equalsIgnoreCase(type)).findFirst()
      .orElseThrow(() -> new IllegalArgumentException("Unknown source type for " + name + ", supported types are "
        + sourceFactories.stream().map(sf -> sf.type().getName()).collect(Collectors.toList()))
    );

    return mySourceFactory.createPublisher(conf).thenAccept(p -> sources.put(name, p));
  }


    private CompletionStage createSinkFromConfig(String name, Map<String, String> conf) {
      // Extract the type and throw an exception if missing
      String type = Optional.ofNullable(conf.get("type")).map(Object::toString)
        .orElseThrow(() -> new IllegalArgumentException("Invalid sink, no type for " + name));

      // Look for the factory and throw an exception if missing
      SubscriberFactory mySinkFactory = sinkFactories.stream().filter(factory -> factory.type().getName().equalsIgnoreCase(type)).findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Unknown sink type for " + name + ", supported types are "
          + sinkFactories.stream().map(sf -> sf.type().getName()).collect(Collectors.toList()))
        );

      return mySinkFactory.createSubscriber(conf).thenAccept(p -> sinks.put(name, p));
  }
}
