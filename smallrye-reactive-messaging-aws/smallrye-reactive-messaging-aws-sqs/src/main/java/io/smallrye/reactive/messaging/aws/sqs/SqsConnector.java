package io.smallrye.reactive.messaging.aws.sqs;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING_AND_OUTGOING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.OUTGOING;
import static io.smallrye.reactive.messaging.aws.serialization.SerializationResolver.resolveDeserializer;
import static io.smallrye.reactive.messaging.aws.serialization.SerializationResolver.resolveSerializer;
import static io.smallrye.reactive.messaging.aws.sqs.client.SqsClientFactory.createSqsClient;
import static io.smallrye.reactive.messaging.aws.sqs.i18n.SqsExceptions.ex;
import static io.smallrye.reactive.messaging.aws.sqs.i18n.SqsLogging.log;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;

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
import io.smallrye.reactive.messaging.aws.serialization.Deserializer;
import io.smallrye.reactive.messaging.aws.serialization.Serializer;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.health.HealthReporter;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.vertx.mutiny.core.Vertx;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SqsException;

@ApplicationScoped
@Connector(SqsConnector.CONNECTOR_NAME)
// common
@ConnectorAttribute(name = "queue", type = "string", direction = INCOMING_AND_OUTGOING, description = "Set the SQS queue. If not set, the channel name is used")
@ConnectorAttribute(name = "health-enabled", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Whether health reporting is enabled (default) or disabled", defaultValue = "true")
@ConnectorAttribute(name = "tracing-enabled", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Whether tracing is enabled (default) or disabled", defaultValue = "true")

@ConnectorAttribute(name = "endpoint-override", type = "string", direction = INCOMING_AND_OUTGOING, description = "Configure the endpoint with which the SDK should communicate.")
@ConnectorAttribute(name = "region", type = "string", direction = INCOMING_AND_OUTGOING, description = "Configure the region with which the SDK should communicate.")

@ConnectorAttribute(name = "queue-resolver.queue-owner-aws-account-id", type = "string", direction = INCOMING_AND_OUTGOING, description = "During queue url resolving it is possible to overwrite the queue owner.")

@ConnectorAttribute(name = "create-queue.enabled", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Whether automatic queue creation is enabled or disabled (default)", defaultValue = "false")
// TODO: Not sure how to load maps. Maybe: key:value,key:value. It is not very efficient, but it is cached by the SqsTargetResolver. So maybe it does not matter.
//  Otherwise, I would need to wrap the config so that I can keep using the easy generated way, but also overwrite methods, to add config internal caching.
@ConnectorAttribute(name = "create-queue.attributes", type = "string", direction = INCOMING_AND_OUTGOING, description = "A comma separated list of attributes for queue creation. Default empty.", defaultValue = "")
@ConnectorAttribute(name = "create-queue.tags", type = "string", direction = INCOMING_AND_OUTGOING, description = "A comma separated list of tags for queue creation. Default empty.", defaultValue = "")
@ConnectorAttribute(name = "create-queue.dead-letter-queue.enabled", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Whether automatic dead-letter queue creation is enabled or disabled (default)", defaultValue = "false")
@ConnectorAttribute(name = "create-queue.dead-letter-queue.prefix", type = "string", direction = INCOMING_AND_OUTGOING, description = "Dead-letter queue name prefix", defaultValue = "")
@ConnectorAttribute(name = "create-queue.dead-letter-queue.suffix", type = "string", direction = INCOMING_AND_OUTGOING, description = "Dead-letter queue name suffix", defaultValue = "-dlq")
@ConnectorAttribute(name = "create-queue.dead-letter-queue.max-receive-count", type = "int", direction = INCOMING_AND_OUTGOING, description = "The number of times a message is delivered to the source queue before being moved to the dead-letter queue. Default: 10. When the ReceiveCount for a message exceeds the maxReceiveCount for a queue, Amazon SQS moves the message to the dead-letter-queue.", defaultValue = "10")
@ConnectorAttribute(name = "create-queue.dead-letter-queue.attributes", type = "string", direction = INCOMING_AND_OUTGOING, description = "A comma separated list of attributes for queue creation. Default empty.", defaultValue = "")
@ConnectorAttribute(name = "create-queue.dead-letter-queue.tags", type = "string", direction = INCOMING_AND_OUTGOING, description = "A comma separated list of tags for queue creation. Default empty.", defaultValue = "")

// outgoing
@ConnectorAttribute(name = "send.batch.enabled", type = "boolean", direction = OUTGOING, description = "Send messages in batches.", defaultValue = "false")

@ConnectorAttribute(name = "serialization.enabled", type = "boolean", direction = OUTGOING, description = "Enable serialization. Default: false", defaultValue = "false")
@ConnectorAttribute(name = "serialization.identifier", type = "string", direction = OUTGOING, description = "Name of the @Identifier to use. If not specified the channel name is used.")

// incoming
@ConnectorAttribute(name = "max-number-of-messages", type = "int", direction = INCOMING, description = "The maximum number of messages to return. Amazon SQS never returns more messages than this value (however, fewer messages might be returned). Valid values: 1 to 10. Default: 10.", defaultValue = "10")
@ConnectorAttribute(name = "wait-time-seconds", type = "int", direction = INCOMING, description = "The duration (in seconds) for which the call waits for a message to arrive in the queue before returning. If a message is available, the call returns sooner than WaitTimeSeconds. If no messages are available and the wait time expires, the call returns successfully with an empty list of messages. Default 20s.", defaultValue = "20")
@ConnectorAttribute(name = "visibility-timeout", type = "int", direction = INCOMING, description = "The duration (in seconds) that the received messages are hidden from subsequent retrieve requests after being retrieved by a request. Default 15s.", defaultValue = "15")
@ConnectorAttribute(name = "attribute-names", type = "string", direction = INCOMING, description = "A comma separated list of attributes that need to be returned along with each message. Default empty.", defaultValue = "")
@ConnectorAttribute(name = "message-attribute-names", type = "string", direction = INCOMING, description = "A comma separated list of message attributes that need to be returned along with each message. Default empty.", defaultValue = "")

@ConnectorAttribute(name = "delete.batch.enabled", type = "boolean", direction = INCOMING, description = "Delete/confirm messages in batches.", defaultValue = "false")
@ConnectorAttribute(name = "delete.batch.max-size", type = "int", direction = INCOMING, description = "The maximum number of messages to delete in a batch. Valid values: 1 to 10. Default: 10.", defaultValue = "10")
@ConnectorAttribute(name = "delete.batch.max-delay", type = "int", direction = INCOMING, description = "The maximum number of seconds to wait for a batch. Needs to be configured lower than message visibility-timeout. Default: 3.", defaultValue = "3")

@ConnectorAttribute(name = "deserialization.enabled", type = "boolean", direction = INCOMING, description = "Enable deserialization. Default: false", defaultValue = "false")
@ConnectorAttribute(name = "deserialization.identifier", type = "string", direction = INCOMING, description = "Name of the @Identifier to use. If not specified the channel name is used.")

public class SqsConnector implements InboundConnector, OutboundConnector, HealthReporter {

    public static final String CONNECTOR_NAME = "smallrye-aws-sqs";

    private final Map<String, SqsAsyncClient> clients = new ConcurrentHashMap<>();
    private final List<SqsChannel> channels = new CopyOnWriteArrayList<>();

    @Inject
    private ExecutionHolder executionHolder;

    @Inject
    Instance<JsonMapping> jsonMapper;
    private JsonMapping jsonMapping;

    @Inject
    @Any
    Instance<Serializer> messageSerializer;

    @Inject
    @Any
    Instance<Deserializer> messageDeserializer;

    private Vertx vertx;
    private SqsTargetResolver targetResolver;

    @PostConstruct
    public void init() {
        this.vertx = executionHolder.vertx();
        this.targetResolver = new SqsTargetResolver();

        if (jsonMapper.isUnsatisfied()) {
            log.debug(
                    "No mapping modules (-jsonb or -jackson) defined. Fallback to toString() and String.");
        } else if (jsonMapper.isAmbiguous()) {
            log.warn(
                    "Please select only one of the additional mapping modules (-jsonb or -jackson) to be able to (de)serialize JSON messages.");
            this.jsonMapping = jsonMapper.stream().findFirst()
                    .orElseThrow(() -> new RuntimeException("Unable to find JSON Mapper"));
        } else {
            this.jsonMapping = jsonMapper.get();
        }
    }

    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        SqsConnectorIncomingConfiguration ic = new SqsConnectorIncomingConfiguration(config);

        SqsAsyncClient client = clients.computeIfAbsent(ic.getChannel(), ignored -> createSqsClient(ic, vertx));

        final Deserializer deserializer = getDeserializer(ic);

        try {
            SqsIncomingChannel channel = new SqsIncomingChannel(
                    new SqsClientHolder<>(client, vertx, ic, targetResolver, null, deserializer));
            channels.add(channel);
            return channel.getPublisher();
        } catch (SqsException e) {
            throw ex.illegalStateUnableToBuildProducer(e);
        }
    }

    private Deserializer getDeserializer(final SqsConnectorIncomingConfiguration ic) {
        if (Boolean.TRUE.equals(ic.getDeserializationEnabled())) {
            return resolveDeserializer(messageDeserializer, ic.getDeserializationIdentifier().orElse(ic.getChannel()),
                    ic.getChannel(), jsonMapping);
        } else {
            return payload -> payload;
        }
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        SqsConnectorOutgoingConfiguration oc = new SqsConnectorOutgoingConfiguration(config);

        SqsAsyncClient client = clients.computeIfAbsent(oc.getChannel(), ignored -> createSqsClient(oc, vertx));

        final Serializer serializer = getSerializer(oc);

        try {
            SqsOutgoingChannel channel = new SqsOutgoingChannel(
                    new SqsClientHolder<>(client, vertx, oc, targetResolver, serializer, null));
            channels.add(channel);
            return channel.getSubscriber();
        } catch (SqsException e) {
            throw ex.illegalStateUnableToBuildConsumer(e);
        }
    }

    private Serializer getSerializer(final SqsConnectorOutgoingConfiguration oc) {
        if (Boolean.TRUE.equals(oc.getSerializationEnabled())) {
            return resolveSerializer(messageSerializer, oc.getSerializationIdentifier().orElse(oc.getChannel()),
                    oc.getChannel(), jsonMapping);
        } else {
            return String::valueOf;
        }
    }

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        channels.forEach(SqsChannel::close);
        for (SqsAsyncClient client : clients.values()) {
            try {
                client.close();
            } catch (SqsException e) {
                log.unableToCloseClient(e);
            }
        }
        channels.clear();
        clients.clear();
    }
}
