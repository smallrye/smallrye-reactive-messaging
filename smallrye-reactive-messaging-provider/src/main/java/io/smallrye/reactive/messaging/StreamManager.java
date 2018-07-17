package io.smallrye.reactive.messaging;

import io.reactivex.Completable;
import io.smallrye.reactive.messaging.spi.SubscriberFactory;
import io.smallrye.reactive.messaging.spi.PublisherFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.config.Config;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Look for stream factories and create instances.
 */
//TODO Extract interface
@ApplicationScoped
public class StreamManager {

  private static final Logger LOGGER = LogManager.getLogger(StreamManager.class);
  public static final String SOURCE_CONFIG_PREFIX = "smallrye.messaging.source.";
  public static final String SINK_CONFIG_PREFIX = "smallrye.messaging.sink.";


  private final List<PublisherFactory> sourceFactories;
  private final List<SubscriberFactory> sinkFactories;
  private final Config config;
  private final ReactiveMessagingExtension extension;

  private Map<String, Publisher<?>> sources = new HashMap<>();
  private Map<String, Subscriber<?>> sinks = new HashMap<>();

  @Inject
  public StreamManager(@Any Instance<PublisherFactory> sourceFactories, @Any Instance<SubscriberFactory> sinkFactories, @Any Instance<Config> config, ReactiveMessagingExtension extension) {
    this.sourceFactories = sourceFactories.stream().collect(Collectors.toList());
    this.sinkFactories = sinkFactories.stream().collect(Collectors.toList());
    this.config = config.get();
    this.extension = extension;
  }

//  Completable initialized() {
//    LOGGER.info("Stream manager initializing...");
//    Set<String> sourceNames = StreamSupport.stream(config.getPropertyNames().spliterator(), false)
//      .filter(key -> key.startsWith(SOURCE_CONFIG_PREFIX))
//      .map(key -> key.substring(SOURCE_CONFIG_PREFIX.length()))
//      .collect(Collectors.toSet());
//
//    Set<String> sinkNames = StreamSupport.stream(config.getPropertyNames().spliterator(), false)
//      .filter(key -> key.startsWith(SINK_CONFIG_PREFIX))
//      .map(key -> key.substring(SINK_CONFIG_PREFIX.length()))
//      .collect(Collectors.toSet());
//
//    List<Completable> tasks = new ArrayList<>();
//    sourceNames.forEach(name -> tasks.add(createSourceFromConfig(name)));
//    sinkNames.forEach(name -> tasks.add(createSinkFromConfig(name)));
//
//    return Completable.merge(tasks).doOnComplete(() -> {
//      LOGGER.info("Sources created during initialization: {}", sources.keySet());
//      LOGGER.info("Sinks created during initialization: {}", sinks.keySet());
//    });
//  }

//  private Completable createSourceFromConfig(String name) {
//    // Collect all entries starting with this name
//    HashMap<String, String> sourceConfiguration = StreamSupport.stream(config.getPropertyNames().spliterator(), false)
//      .filter(s -> s.startsWith(SOURCE_CONFIG_PREFIX + name))
//      .collect(HashMap::new, (map, key) -> map.put(key, config.getValue(key, String.class)), Map::putAll);
//
//    // Extract the type and throw an exception if missing
//    String type = sourceConfiguration.get("type");
//    if (type == null) {
//      throw new IllegalArgumentException("Invalid source, no type for " + name);
//    }
//
//    // Look for the factory and throw an exception if missing
//    PublisherFactory mySourceFactory = sourceFactories.stream().filter(factory -> factory.type().getName().equalsIgnoreCase(type)).findFirst()
//      .orElseThrow(() -> new IllegalArgumentException("Unknown source type for " + name + ", supported types are "
//        + sourceFactories.stream().map(sf -> sf.type().getClass().getName()).collect(Collectors.toList()))
//    );
//
//    return mySourceFactory.create(extension.vertx(), name, sourceConfiguration).doOnSuccess(p -> sources.put(name, p)).ignoreElement();
//  }
//
//  private Completable createSinkFromConfig(String name) {
//    // Collect all entries starting with this name
//    HashMap<String, String> sinkConfiguration = StreamSupport.stream(config.getPropertyNames().spliterator(), false)
//      .filter(s -> s.startsWith(SINK_CONFIG_PREFIX + name))
//      .collect(HashMap::new, (map, key) -> map.put(key, config.getValue(key, String.class)), Map::putAll);
//
//    // Extract the type and throw an exception if missing
//    String type = sinkConfiguration.get("type");
//    if (type == null) {
//      throw new IllegalArgumentException("Invalid sink, no type for " + name);
//    }
//
//    // Look for the factory and throw an exception if missing
//    SubscriberFactory mySinkFactory = sinkFactories.stream().filter(factory -> factory.type().getName().equalsIgnoreCase(type)).findFirst().orElseThrow(
//      () -> new IllegalArgumentException("Unknown sink type for " + name + ", supported types are "
//        + sinkFactories.stream().map(sf -> sf.type().getClass().getName()).collect(Collectors.toList()))
//    );
//
//    return mySinkFactory.create(extension.vertx(), name, sinkConfiguration).doOnSuccess(p -> sinks.put(name, p)).ignoreElement();
//  }


  public Publisher getSource(String name) {
    return sources.get(name);
  }

  public Subscriber getSink(String name) {
    return sinks.get(name);
  }

  public void register(Publisher<?> source, String name) {
    sources.put(name, source);
  }
}
