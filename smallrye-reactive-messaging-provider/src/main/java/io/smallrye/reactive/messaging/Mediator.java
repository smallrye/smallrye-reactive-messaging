package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import io.reactivex.processors.UnicastProcessor;
import io.smallrye.reactive.messaging.utils.StreamConnector;
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
import java.util.concurrent.CompletionStage;

public class Mediator {

  private static final Logger LOGGER = LogManager.getLogger(Mediator.class);

  private final MediatorConfiguration config;
  private final StreamConnector output;
  private final StreamConnector input;
  private Flowable<? extends Message> flowable;
  private Object instance;

  Mediator(MediatorConfiguration configuration) {
    this.config = configuration;
    if (configuration.isPublisher()) {
      this.output = new StreamConnector<>(getConfiguration().methodAsString() + "[output:" + configuration.getOutgoing() + "]");
    } else {
      this.output = null;
    }
    if (configuration.isSubscriber()) {
      this.input = new StreamConnector<>(getConfiguration().methodAsString() + "[input: " + configuration.getIncoming() + "]");
    } else {
      this.input = null;
    }
  }


  @SuppressWarnings("unchecked")
  public void initialize(Object instance) {
    this.instance = instance;

    if (this.input != null) {
      Flowable<? extends Message> flow = Flowable.fromPublisher(input);
      if (config.consumeAsStream()) {
        boolean consumePayloads = config.isConsumingPayloads();
        boolean producingPayloads = config.isProducingPayloads();
        Object[] args = computeArgumentForMethod(flow, consumePayloads);
        Object result = invoke(args);
        if (result instanceof Processor) {
          Processor processor = (Processor) result;
          if (consumePayloads) {
            flowable = Flowable.fromPublisher(
              ReactiveStreams.fromPublisher(flow.map(Message::getPayload)).via(processor).buildRs());
          } else {
            flowable = Flowable.fromPublisher(
              ReactiveStreams.fromPublisher(flow).via(processor).buildRs());
          }
        } else if (result instanceof PublisherBuilder) {
          flowable = Flowable.fromPublisher(((PublisherBuilder) result).buildRs());
        } else if (result instanceof Publisher) {
          flowable = Flowable.fromPublisher((Publisher) result);
        } else if (result instanceof ProcessorBuilder) {
          ProcessorBuilder pb = (ProcessorBuilder) result;
          if (consumePayloads) {
            flowable = Flowable.fromPublisher(ReactiveStreams.fromPublisher(flow.map(Message::getPayload)).via(pb).buildRs());
          } else {
            flowable = Flowable.fromPublisher(ReactiveStreams.fromPublisher(flow).via(pb).buildRs());
          }
        }
        if (producingPayloads) {
          flowable = flowable
            // The cast is used to indicate that we are not expecting a message, but objects at that point.
            // without the mapper cannot be called (cast exception)
            .cast(Object.class)
            .map(Message::of);
        }
      } else {
        // Receive individual items
        // Check if the method expect a payload or a message
        // Same for the returned type
        boolean consumePayload = ! MediatorConfiguration.isClassASubTypeOf(config.getParameterType(), Message.class);
        boolean produceACompletionStage = MediatorConfiguration.isClassASubTypeOf(config.getReturnType(), CompletionStage.class);
        boolean producePayload = (! produceACompletionStage  && ! MediatorConfiguration.isClassASubTypeOf(config.getReturnType(), Message.class))
          ||  (produceACompletionStage  && ! config.isReturningCompletionStageOfMessage());

        flowable = flow
          .compose(f -> {
            if (consumePayload) {
              return f.map(Message::getPayload);
            }
            return f.cast(Object.class);
          })
          .flatMap(item ->
            Flowable.just(item)
              .map(this::invokeMethodWithItem)
              .compose(f -> {
                if (produceACompletionStage) {
                  return f.flatMap(cs -> fromCompletionStage((CompletionStage) cs), 1);
                } else {
                  return f;
                }
              })
              .retry(1)) // TODO make it configurable
          .compose(f -> {
            if (producePayload) {
              return f.map(Message::of);
            } else {
              return f.cast(Message.class);
            }
          });
      }
    } else {
      flowable = createPublisher();
    }

  }

  private Object invokeMethodWithItem(Object item) {
    return invoke(item);
  }

  private Object[] computeArgumentForMethod(Flowable<? extends Message> source, boolean consumeItems) {
    Method method = config.getMethod();
    if (method.getParameterCount() == 0) {
      return new Object[0];
    }
    // Only supported case right now is 1
    Class<?> paramClass = method.getParameterTypes()[0];
    if (MediatorConfiguration.isClassASubTypeOf(paramClass, PublisherBuilder.class)) {
      if (consumeItems) {
        return new Object[] {ReactiveStreams.fromPublisher(source).map(Message::getPayload)};
      } else {
        return new Object[]{ReactiveStreams.fromPublisher(source)};
      }
    } else if (MediatorConfiguration.isClassASubTypeOf(paramClass, Publisher.class)) {
      if (consumeItems) {
        return new Object[] {source.map(Message::getPayload)};
      } else {
        return new Object[]{source};
      }
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
    Object result = invoke();
    boolean mustWrap = config.isProducingPayloads();
    if (result == null) {
      throw new IllegalArgumentException("The method " + config.methodAsString() + " must not return `null` to produce a Publisher");
    }
    if (result instanceof Flowable) {
      if (mustWrap) {
        return ((Flowable) result).map(Message::of);
      } else {
        return (Flowable) result;
      }
    }
    if (result instanceof Publisher) {
      if (mustWrap) {
        return Flowable.fromPublisher((Publisher) result).map(Message::of);
      } else {
        return Flowable.fromPublisher((Publisher) result);
      }
    }
    if (result instanceof PublisherBuilder) {
      if (mustWrap) {
        return Flowable.fromPublisher(((PublisherBuilder) result).buildRs()).map(Message::of);
      } else {
        return Flowable.fromPublisher(((PublisherBuilder) result).buildRs());
      }

    }
    throw new IllegalArgumentException("Not support result type to create a Publisher: " + result.getClass());
  }

  public MediatorConfiguration getConfiguration() {
    return config;
  }

  public Publisher<? extends Message> getOutput() {
    return output;
  }

  public Subscriber<? extends Message> getInput() {
    return input;
  }

  /**
   * Returns a Flowable that emits the completion when the CompletionStage receives a value. Propagate the error when an error is passed.
   * @param <T> the value type
   * @param future the source CompletionStage instance
   * @return the new Completable instance
   */
  public static <T> Flowable<T> fromCompletionStage(CompletionStage<T> future) {
    UnicastProcessor<T> cs = UnicastProcessor.create();

    future.whenComplete((v, e) -> {
      if (e != null) {
        cs.onError(e);
      } else {
        cs.onComplete();
      }
    });

    return cs;
  }

  public void connect(Publisher<? extends Message> publisher) {
    if (input == null) {
      throw new IllegalStateException("Cannot connect to upstream, the mediator does not expect a source");
    }
    LOGGER.info("Connecting {} to upstream '{}' ({})", config.methodAsString(), config.getIncoming(), publisher);
    input.connectUpstream(publisher);
  }

  public void connect(Subscriber<? extends Message> subscriber) {
    if (output == null) {
      throw new IllegalStateException("Cannot connect to downstream, the mediator does not expect a sink");
    }
    LOGGER.info("Connecting {} to downstream '{}' ({})", config.methodAsString(), config.getOutgoing(), subscriber);
    output.connectUpstream(flowable);
    output.connectDownStream(subscriber);
  }
}
