package io.smallrye.reactive.messaging.camel;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.OUTGOING;
import static io.smallrye.reactive.messaging.camel.i18n.CamelExceptions.ex;
import static io.smallrye.reactive.messaging.camel.i18n.CamelLogging.log;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.component.reactive.streams.api.CamelReactiveStreamsService;
import org.apache.camel.component.reactive.streams.engine.DefaultCamelReactiveStreamsServiceFactory;
import org.apache.camel.component.reactive.streams.engine.ReactiveStreamsEngineConfiguration;
import org.apache.camel.spi.Synchronization;
import org.apache.camel.support.DefaultExchange;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Publisher;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction;

@ApplicationScoped
@Connector(CamelConnector.CONNECTOR_NAME)
@ConnectorAttribute(name = "endpoint-uri", description = "The URI of the Camel endpoint (read from or written to)", mandatory = true, type = "string", direction = Direction.INCOMING_AND_OUTGOING)
@ConnectorAttribute(name = "failure-strategy", type = "string", direction = Direction.INCOMING, description = "Specify the failure strategy to apply when a message produced from a Camel exchange is nacked. Values can be `fail` (default) or `ignore`", defaultValue = "fail")
@ConnectorAttribute(name = "merge", direction = OUTGOING, description = "Whether the connector should allow multiple upstreams", type = "boolean", defaultValue = "false")
public class CamelConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

    private static final String REACTIVE_STREAMS_SCHEME = "reactive-streams:";
    public static final String CONNECTOR_NAME = "smallrye-camel";

    @Inject
    private CamelContext camel;

    private CamelReactiveStreamsService reactive;

    @Produces
    public CamelReactiveStreamsService getCamelReactive() {
        if (this.reactive != null) {
            return this.reactive;
        }

        CamelReactiveStreamsService service = camel.hasService(CamelReactiveStreamsService.class);
        if (service != null) {
            log.camelReactiveStreamsServiceAlreadyDefined();
            this.reactive = service;
            return service;
        } else {
            DefaultCamelReactiveStreamsServiceFactory factory = new DefaultCamelReactiveStreamsServiceFactory();
            ReactiveStreamsEngineConfiguration configuration = new ReactiveStreamsEngineConfiguration();

            Config config = ConfigProvider.getConfig();
            config.getOptionalValue(
                    "camel.component.reactive-streams.internal-engine-configuration.thread-pool-max-size",
                    Integer.class).ifPresent(configuration::setThreadPoolMaxSize);
            config.getOptionalValue(
                    "camel.component.reactive-streams.internal-engine-configuration.thread-pool-min-size",
                    Integer.class).ifPresent(configuration::setThreadPoolMinSize);
            config
                    .getOptionalValue("camel.component.reactive-streams.internal-engine-configuration.thread-pool-name",
                            String.class)
                    .ifPresent(configuration::setThreadPoolName);
            this.reactive = factory.newInstance(camel, configuration);
            try {
                this.camel.addService(this.reactive, true, true);
            } catch (Exception e) {
                throw ex.unableToRegisterService(e);
            }
            return reactive;
        }
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        CamelConnectorIncomingConfiguration ic = new CamelConnectorIncomingConfiguration(config);
        String name = ic.getEndpointUri();
        CamelFailureHandler.Strategy strategy = CamelFailureHandler.Strategy.from(ic.getFailureStrategy());
        CamelFailureHandler onNack = createFailureHandler(strategy, ic.getChannel());

        Publisher<Exchange> publisher;
        if (name.startsWith(REACTIVE_STREAMS_SCHEME)) {
            // The endpoint is a reactive streams.
            name = name.substring(REACTIVE_STREAMS_SCHEME.length());
            log.creatingPublisherFromStream(name);
            publisher = getCamelReactive().fromStream(name);
        } else {
            log.creatingPublisherFromEndpoint(name);
            publisher = getCamelReactive().from(name);
        }

        return ReactiveStreams.fromPublisher(publisher)
                .map(ex -> new CamelMessage<>(ex, onNack));
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        String name = new CamelConnectorOutgoingConfiguration(config).getEndpointUri();

        SubscriberBuilder<? extends Message<?>, Void> subscriber;
        if (name.startsWith(REACTIVE_STREAMS_SCHEME)) {
            // The endpoint is a reactive streams.
            name = name.substring(REACTIVE_STREAMS_SCHEME.length());
            log.creatingSubscriberFromStream(name);
            subscriber = ReactiveStreams.<Message<?>> builder()
                    .map(this::createExchangeFromMessage)
                    .to(getCamelReactive().streamSubscriber(name));
        } else {
            log.creatingSubscriberFromEndpoint(name);
            subscriber = ReactiveStreams.<Message<?>> builder()
                    .map(this::createExchangeFromMessage)
                    .to(getCamelReactive().subscriber(name));
        }
        return subscriber;
    }

    private Exchange createExchangeFromMessage(Message<?> message) {
        if (message.getPayload() instanceof Exchange) {
            return (Exchange) message.getPayload();
        }

        OutgoingExchangeMetadata outGoingMetadata = message.getMetadata(OutgoingExchangeMetadata.class).orElse(null);
        IncomingExchangeMetadata incomingMetadata = message.getMetadata(IncomingExchangeMetadata.class).orElse(null);
        DefaultExchange exchange = new DefaultExchange(camel);
        if (outGoingMetadata != null) {
            outGoingMetadata.getProperties().forEach(exchange::setProperty);
            if (outGoingMetadata.getExchangePattern() != null) {
                exchange.setPattern(outGoingMetadata.getExchangePattern());
            }

            outGoingMetadata.getHeaders().forEach(exchange.getIn()::setHeader);
        }
        // In case of message processor pattern, the incoming headers should be forwarded too
        if (incomingMetadata != null && incomingMetadata.getExchange().getIn() != null
                && incomingMetadata.getExchange().getIn().getHeaders() != null) {
            incomingMetadata.getExchange().getIn().getHeaders().forEach(exchange.getIn()::setHeader);
        }

        exchange.getIn().setBody(message.getPayload());
        exchange.addOnCompletion(new Synchronization() {
            @Override
            public void onComplete(Exchange exchange) {
                message.ack();
            }

            @Override
            public void onFailure(Exchange exchange) {
                log.exchangeFailed(exchange.getException());
                message.nack(exchange.getException());
            }
        });
        return exchange;
    }

    private CamelFailureHandler createFailureHandler(CamelFailureHandler.Strategy strategy, String channel) {
        switch (strategy) {
            case IGNORE:
                return new CamelIgnoreFailure(channel);
            case FAIL:
                return new CamelFailStop(channel);
            default:
                throw ex.illegalArgumentUnknownStrategy(strategy);
        }
    }
}
