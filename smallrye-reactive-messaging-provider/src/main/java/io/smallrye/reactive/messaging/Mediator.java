package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.smallrye.reactive.messaging.utils.ConnectableProcessor;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.lang.reflect.Method;

public class Mediator {

  private static final Logger LOGGER = LogManager.getLogger(Mediator.class);

  private final MediatorConfiguration config;
  private final StreamRegistry registry;
  private final ConnectableProcessor output;
  private Flowable<? extends Message> source;
  private Subscriber<? extends Message> subscriber;
  private Flowable<? extends Message> flowable;
  private Object instance;

  Mediator(MediatorConfiguration configuration, StreamRegistry registry) {
    this.config = configuration;
    this.registry = registry;
    this.output = new ConnectableProcessor<>();
  }

  private FlowableTransformer wrap(boolean produceRawItems) {
    return flow -> {
      if (produceRawItems) {
        return flow.map(DefaultMessage::create);
      }
      return flow;
    };
  }

  private FlowableTransformer unwrap(boolean consumeRawItems) {
    return flow -> {
      if (consumeRawItems) {
        return flow
          .cast(Message.class)
          .map(i -> ((Message) i).getPayload());
      }
      return flow;
    };
  }


  public void initialize(Object instance) {
    this.instance = instance;
    if (config.isSubscriber()) {
      lookForSourceOrDie(config.getIncomingTopic());
    }
    if (config.isPublisher()) {
      lookForSinkOrDie(config.getOutgoingTopic());
    }

    if (source != null) {
      if (config.consumeAsStream()) {
        Object[] args = computeArgumentForMethod(source);
        Object result = invoke(args);
        boolean consumeItem = config.consumeItems();
        boolean produceItem = config.produceItems();
        if (result instanceof Processor) {
          Processor processor = (Processor) result;
          flowable = Flowable.fromPublisher(
            ReactiveStreams.fromPublisher(source).via(processor).buildRs());
        } else if (result instanceof PublisherBuilder) {
          flowable = Flowable.fromPublisher(((PublisherBuilder) result).buildRs());
        } else if (result instanceof Publisher) {
          flowable = Flowable.fromPublisher((Publisher) result);
        } else if (result instanceof ProcessorBuilder) {
          ProcessorBuilder pb = (ProcessorBuilder) result;
          flowable = Flowable.fromPublisher(ReactiveStreams.fromPublisher(source).via(pb).buildRs());
        }
      } else {
        // Receive individual items
        // Check if the method expect a payload or a message
        // Same for the returned type
        boolean consumePayload = ! MediatorConfiguration.isClassASubTypeOf(config.getParameterType(), Message.class);
        boolean producePayload = ! MediatorConfiguration.isClassASubTypeOf(config.getReturnType(), Message.class);

        flowable = source
          .compose(flow -> {
            if (consumePayload) {
              return flow.map(Message::getPayload).cast(Object.class);
            }
            return flow.cast(Object.class);
          })
          .map(this::invokeMethodWithItem)
          .compose(flow -> {
            if (producePayload) {
              return flow.map(DefaultMessage::create);
            } else {
              return flow.cast(Message.class);
            }
          });
      }
    } else {
      //TODO Test types
      flowable = createPublisher();
    }

  }

  private Object invokeMethodWithItem(Object item) {
    return invoke(item);
  }

  private Object[] computeArgumentForMethod(Flowable<?> source) {
    Method method = config.getMethod();
    if (method.getParameterCount() == 0) {
      return new Object[0];
    }
    // Only supported case right now is 1
    Class<?> paramClass = method.getParameterTypes()[0];
    if (MediatorConfiguration.isClassASubTypeOf(paramClass, PublisherBuilder.class)) {
      return new Object[] {ReactiveStreams.fromPublisher(source)};
    } else if (MediatorConfiguration.isClassASubTypeOf(paramClass, Publisher.class)) {
      return new Object[] { source };
    }

    throw new IllegalArgumentException("Not supported parameter type: " + paramClass.getName());

  }

  private <T> T invoke(Object... args) {
    try {
      Method method = config.getMethod();
      return (T) method.invoke(instance, args);
    } catch (Exception e) {
      // TODO better error reporting
      throw new RuntimeException(e);
    }
  }

  private Flowable createPublisher() {
    return invoke();
  }

  private void lookForSourceOrDie(String name) {
    this.source = registry.getPublisher(name)
      .orElseThrow(() -> new IllegalStateException("Cannot find publisher named: " + name));
  }

  private void lookForSinkOrDie(String name) {
    if (StringUtils.isBlank(name)) {
      return;
    }
    subscriber = registry.getSubscriber(name)
      .orElseThrow(() -> new IllegalStateException("Cannot find subscriber named: " + name));
  }

  public void run() {
    if (subscriber != null) {
      output.subscribe(subscriber);
    }
    flowable
      .doOnError(t -> LOGGER.error("Error caught when executing {}", config.methodAsString(), t))
      .subscribe(output);
  }

  public MediatorConfiguration getConfiguration() {
    return config;
  }

  public Publisher<? extends Message> getPublisher() {
    return output;
  }
}
