package io.smallrye.reactive.messaging.aws.sqs;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Reception;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.health.HealthReporter;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;
import io.smallrye.reactive.messaging.providers.helpers.VertxJsonMapping;
import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(SqsConnector.CONNECTOR_NAME)
@ConnectorAttribute(name = "queue", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The name of the SQS queue, defaults to channel name if not provided")
@ConnectorAttribute(name = "queue.url", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The url of the SQS queue")
@ConnectorAttribute(name = "region", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The name of the SQS region")
@ConnectorAttribute(name = "endpoint-override", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The endpoint override")
@ConnectorAttribute(name = "credentials-provider", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The credential provider to be used in the client")
@ConnectorAttribute(name = "health-enabled", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "Whether health reporting is enabled (default) or disabled", defaultValue = "true")

@ConnectorAttribute(name = "group.id", type = "string", direction = ConnectorAttribute.Direction.OUTGOING, description = "When set, sends messages with the specified group id")

@ConnectorAttribute(name = "wait-time-seconds", type = "int", direction = ConnectorAttribute.Direction.INCOMING, description = "The maximum amount of time in seconds to wait for messages to be received", defaultValue = "1")
@ConnectorAttribute(name = "max-number-of-messages", type = "int", direction = ConnectorAttribute.Direction.INCOMING, description = "The maximum number of messages to receive", defaultValue = "10")
@ConnectorAttribute(name = "visibility-timeout", type = "int", direction = ConnectorAttribute.Direction.INCOMING, description = "The duration in seconds that the received messages are hidden from subsequent retrieve requests after being retrieved by a receive request")
@ConnectorAttribute(name = "receive.request.message-attribute-names", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "The message attribute names to retrieve when receiving messages.")
@ConnectorAttribute(name = "receive.request.customizer", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "The identifier for the bean implementing a customizer to receive requests, defaults to channel name if not provided")
@ConnectorAttribute(name = "receive.request.retries", type = "long", direction = ConnectorAttribute.Direction.INCOMING, description = "If set to a positive number, the connector will try to retry the request that was not delivered successfully (with a potentially transient error) until the number of retries is reached. If set to 0, retries are disabled.", defaultValue = "2147483647")
@ConnectorAttribute(name = "receive.request.pause.resume", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING, description = "Whether the polling must be paused when the application does not request items and resume when it does. This allows implementing back-pressure based on the application capacity. Note that polling is not stopped, but will not retrieve any records when paused.", defaultValue = "true")
@ConnectorAttribute(name = "ack.delete", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING, description = "Whether the acknowledgement deletes the message from the queue", defaultValue = "true")
public class SqsConnector implements InboundConnector, OutboundConnector, HealthReporter {

    @Inject
    private SqsManager sqsManager;

    @Inject
    ExecutionHolder executionHolder;

    @Inject
    @Any
    Instance<SqsReceiveMessageRequestCustomizer> customizers;

    @Inject
    Instance<JsonMapping> jsonMappers;

    Vertx vertx;

    private static final List<SqsInboundChannel> INBOUND_CHANNELS = new CopyOnWriteArrayList<>();
    private static final List<SqsOutboundChannel> OUTBOUND_CHANNELS = new CopyOnWriteArrayList<>();

    public static final String CONNECTOR_NAME = "smallrye-sqs";
    public static final String CLASS_NAME_ATTRIBUTE = "_classname";
    private JsonMapping jsonMapping;

    @PostConstruct
    void init() {
        this.vertx = executionHolder.vertx();
        this.jsonMapping = jsonMappers.isUnsatisfied() ? new VertxJsonMapping() : jsonMappers.get();
    }

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        INBOUND_CHANNELS.forEach(SqsInboundChannel::close);
        OUTBOUND_CHANNELS.forEach(SqsOutboundChannel::close);
    }

    @Override
    public Publisher<? extends Message<?>> getPublisher(Config config) {
        var conf = new SqsConnectorIncomingConfiguration(config);
        var customizer = CDIUtils.getInstanceById(customizers, conf.getReceiveRequestCustomizer().orElse(conf.getChannel()),
                () -> null);
        var channel = new SqsInboundChannel(conf, vertx, sqsManager, customizer, jsonMapping);
        INBOUND_CHANNELS.add(channel);
        return channel.getStream();
    }

    @Override
    public Subscriber<? extends Message<?>> getSubscriber(Config config) {
        var conf = new SqsConnectorOutgoingConfiguration(config);
        var channel = new SqsOutboundChannel(conf, sqsManager, jsonMapping);
        OUTBOUND_CHANNELS.add(channel);
        return channel.getSubscriber();
    }

    @Override
    public HealthReport getLiveness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (SqsInboundChannel channel : INBOUND_CHANNELS) {
            channel.isAlive(builder);
        }
        for (SqsOutboundChannel channel : OUTBOUND_CHANNELS) {
            channel.isAlive(builder);
        }
        return builder.build();
    }
}
