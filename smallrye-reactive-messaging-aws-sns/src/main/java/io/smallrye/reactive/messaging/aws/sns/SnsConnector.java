package io.smallrye.reactive.messaging.aws.sns;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.OUTGOING;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.health.HealthReporter;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.helpers.VertxJsonMapping;
import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(SnsConnector.CONNECTOR_NAME)
@ConnectorAttribute(name = "topic", type = "string", direction = OUTGOING, description = "The name of the SNS topic, defaults to channel name if not provided")
@ConnectorAttribute(name = "topic.arn", type = "string", direction = OUTGOING, description = "The arn of the SNS topic")
@ConnectorAttribute(name = "region", type = "string", direction = OUTGOING, description = "The name of the SNS region, defaults to AWS region resolver")
@ConnectorAttribute(name = "endpoint-override", type = "string", direction = OUTGOING, description = "The endpoint override")
@ConnectorAttribute(name = "credentials-provider", type = "string", direction = OUTGOING, description = "The credential provider to be used in the client, defaults to AWS default provider chain")

@ConnectorAttribute(name = "health-enabled", type = "boolean", direction = OUTGOING, description = "Whether health reporting is enabled (default) or disabled", defaultValue = "true")

@ConnectorAttribute(name = "batch", type = "boolean", direction = OUTGOING, description = "When set, sends messages in batches of maximum 10 messages", defaultValue = "false")
@ConnectorAttribute(name = "batch-size", type = "int", direction = OUTGOING, description = "In batch send mode, the maximum number of messages to include in batch, currently SNS maximum is 10 messages", defaultValue = "10")
@ConnectorAttribute(name = "batch-delay", type = "int", direction = OUTGOING, description = "In batch send mode, the maximum delay in milliseconds to wait for messages to be included in the batch. Defaults to 3000 ms", defaultValue = "3000")

@ConnectorAttribute(name = "message-structure", type = "string", direction = OUTGOING, description = "Set to json if you want to send a different message for each protocol.")

@ConnectorAttribute(name = "group.id", type = "string", direction = OUTGOING, description = "When set, sends messages with the specified group id")

@ConnectorAttribute(name = "email.subject", type = "string", direction = OUTGOING, description = "When set, sends messages with the specified subject")

@ConnectorAttribute(name = "sms.phoneNumber", type = "string", direction = OUTGOING, description = "When set, sends messages to the specified phone number. Not supported in combination with batching.")
public class SnsConnector implements OutboundConnector, HealthReporter {

    public static final String CONNECTOR_NAME = "smallrye-sns";
    public static final String CLASS_NAME_ATTRIBUTE = "_classname";

    @Inject
    private SnsManager snsManager;

    @Inject
    ExecutionHolder executionHolder;

    @Inject
    Instance<JsonMapping> jsonMappers;

    Vertx vertx;

    private JsonMapping jsonMapping;

    private final Map<String, SnsOutboundChannel> outboundChannels = new ConcurrentHashMap<>();

    @PostConstruct
    void init() {
        this.vertx = executionHolder.vertx();
        this.jsonMapping = jsonMappers.isUnsatisfied() ? new VertxJsonMapping() : jsonMappers.get();
    }

    @Override
    public CompletionStage<Void> closeOutgoing(String channel) {
        SnsOutboundChannel outbound = outboundChannels.remove(channel);
        if (outbound != null) {
            outbound.close();
        }
        return CompletableFuture.completedStage(null);
    }

    @Override
    public CompletionStage<Void> terminate() {
        // The channels not closed one by one; the SNS clients are closed by the SnsManager.
        outboundChannels.values().forEach(SnsOutboundChannel::close);
        outboundChannels.clear();
        return CompletableFuture.completedStage(null);
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(final Config config) {
        var conf = new SnsConnectorOutgoingConfiguration(config);
        var channel = new SnsOutboundChannel(conf, snsManager, jsonMapping);
        outboundChannels.put(conf.getChannel(), channel);
        return channel.getSubscriber();
    }

    @Override
    public HealthReport getLiveness() {
        var builder = HealthReport.builder();
        for (SnsOutboundChannel channel : outboundChannels.values()) {
            channel.isAlive(builder);
        }
        return builder.build();
    }
}
