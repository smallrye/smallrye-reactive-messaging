package io.smallrye.reactive.messaging.camel;

import io.reactivex.Flowable;
import io.reactivex.functions.Function;
import io.smallrye.reactive.messaging.spi.PublisherFactory;
import io.smallrye.reactive.messaging.spi.SubscriberFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.component.reactive.streams.api.CamelReactiveStreamsService;
import org.apache.camel.component.reactive.streams.engine.DefaultCamelReactiveStreamsServiceFactory;
import org.apache.camel.component.reactive.streams.engine.ReactiveStreamsEngineConfiguration;
import org.apache.camel.impl.DefaultExchange;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.MessagingProvider;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class CamelMessagingProvider implements PublisherFactory, SubscriberFactory {

  private static final Logger LOGGER = LogManager.getLogger(CamelMessagingProvider.class);

  @Inject
  private CamelContext camel;

  private CamelReactiveStreamsService reactive;

  @Produces
  public CamelReactiveStreamsService getCamelReactive() {
    return reactive;
  }

  @PostConstruct
  @Inject
  public void init(Instance<Config> config) {
    DefaultCamelReactiveStreamsServiceFactory factory = new DefaultCamelReactiveStreamsServiceFactory();
    ReactiveStreamsEngineConfiguration configuration = new ReactiveStreamsEngineConfiguration();
    if (!config.isUnsatisfied()) {
      // TODO Ask ASD about this resolution issue
      Config conf = config.stream().findFirst()
        .orElseThrow(() -> new IllegalStateException("Unable to retrieve the config"));

      conf.getOptionalValue("camel.component.reactive-streams.internal-engine-configuration.thread-pool-max-size", Integer.class)
        .ifPresent(configuration::setThreadPoolMaxSize);

      conf.getOptionalValue("camel.component.reactive-streams.internal-engine-configuration.thread-pool-min-size", Integer.class)
        .ifPresent(configuration::setThreadPoolMinSize);

      conf.getOptionalValue("camel.component.reactive-streams.internal-engine-configuration.thread-pool-name", String.class)
        .ifPresent(configuration::setThreadPoolName);
    }
    this.reactive = factory.newInstance(camel, configuration);
  }

  @Override
  public Class<? extends MessagingProvider> type() {
    return Camel.class;
  }

  @Override
  public CompletionStage<Subscriber<? extends Message>> createSubscriber(Map<String, String> config) {
    String name = config.get("endpoint-uri");
    Objects.requireNonNull(name, "The `endpoint-uri` of the endpoint is required");

    Subscriber<Message> subscriber;
    if (name.startsWith("reactive-streams:")) {
      // The endpoint is a reactive streams.
      name = name.substring("reactive-streams:".length());
      LOGGER.info("Creating subscriber from Camel stream named {}", name);
      subscriber = ReactiveStreams.<Message>builder()
        .map(this::createExchangeFromMessage)
        .to(reactive.streamSubscriber(name))
        .build();
    } else {
      LOGGER.info("Creating publisher from Camel endpoint {}", name);
      subscriber = ReactiveStreams.<Message>builder()
        .map(this::createExchangeFromMessage)
        .to(reactive.subscriber(name))
        .build();
    }
    return CompletableFuture.completedFuture(subscriber);
  }

  private Exchange createExchangeFromMessage(Message message) {
    if (message.getPayload() instanceof Exchange) {
      return (Exchange) message.getPayload();
    }
    Exchange exchange = new DefaultExchange(camel);
    exchange.getIn().setBody(message.getPayload());
    return exchange;
  }

  @Override
  public CompletionStage<Publisher<? extends Message>> createPublisher(Map<String, String> config) {
    String name = config.get("endpoint-uri");
    Objects.requireNonNull(name, "The `endpoint-uri of the endpoint is required");

    Publisher<Exchange> publisher;
    if (name.startsWith("reactive-streams:")) {
      // The endpoint is a reactive streams.
      name = name.substring("reactive-streams:".length());
      LOGGER.info("Creating publisher from Camel stream named {}", name);
      publisher = reactive.fromStream(name);
    } else {
      LOGGER.info("Creating publisher from Camel endpoint {}", name);
      publisher = reactive.from(name);
    }

    return CompletableFuture.completedFuture(
      Flowable.fromPublisher(publisher)
        .map((Function<Exchange, CamelMessage>) CamelMessage::new)
    );
  }
}
