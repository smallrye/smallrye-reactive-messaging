package io.smallrye.reactive.messaging.aws.sqs;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING_AND_OUTGOING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.OUTGOING;
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
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.health.HealthReporter;
import io.smallrye.reactive.messaging.json.JsonMapping;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SqsException;

@ApplicationScoped
@Connector(SqsConnector.CONNECTOR_NAME)
// common
@ConnectorAttribute(name = "queue", type = "string", direction = INCOMING_AND_OUTGOING, description = "Set the SQS queue. If not set, the channel name is used")
@ConnectorAttribute(name = "health-enabled", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "Whether health reporting is enabled (default) or disabled", defaultValue = "true")
@ConnectorAttribute(name = "tracing-enabled", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "Whether tracing is enabled (default) or disabled", defaultValue = "true")
@ConnectorAttribute(name = "create-queue.enabled", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "Whether automatic queue creation is enabled or disabled (default)", defaultValue = "false")
@ConnectorAttribute(name = "create-queue.dlq.enabled", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "Whether automatic dead-letter queue creation is enabled or disabled (default)", defaultValue = "false")
@ConnectorAttribute(name = "create-queue.dlq.prefix", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "Dead-letter queue name prefix", defaultValue = "")
@ConnectorAttribute(name = "create-queue.dlq.suffix", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "Dead-letter queue name suffix", defaultValue = "-dlq")
@ConnectorAttribute(name = "create-queue.dlq.max-receive-count", type = "int", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The number of times a message is delivered to the source queue before being moved to the dead-letter queue. Default: 10. When the ReceiveCount for a message exceeds the maxReceiveCount for a queue, Amazon SQS moves the message to the dead-letter-queue.", defaultValue = "10")

// outgoing
@ConnectorAttribute(name = "send.batch.enabled", type = "boolean", direction = OUTGOING, description = "Send messages in batches.", defaultValue = "false")

// incomming
public class SqsConnector implements InboundConnector, OutboundConnector, HealthReporter {

    static final String CONNECTOR_NAME = "smallrye-aws-sqs";

    private final Map<String, SqsAsyncClient> clients = new ConcurrentHashMap<>();
    private final Map<String, SqsAsyncClient> clientsByChannel = new ConcurrentHashMap<>();
    private final List<SqsOutgoingChannel> outgoingChannels = new CopyOnWriteArrayList<>();
    //    private final List<PulsarIncomingChannel<?>> incomingChannels = new CopyOnWriteArrayList<>();

    @Inject
    Instance<JsonMapping> jsonMapper;
    private JsonMapping jsonMapping;

    @PostConstruct
    public void init() {
        if (jsonMapper.isUnsatisfied()) {
            log.warn(
                    "Please add one of the additional mapping modules (-jsonb or -jackson) to be able to (de)serialize JSON messages.");
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

        SqsAsyncClient client = clients.computeIfAbsent("", ignored -> createSqsClient(ic));
        clientsByChannel.put(ic.getChannel(), client);

        return null;
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        SqsConnectorOutgoingConfiguration oc = new SqsConnectorOutgoingConfiguration(config);

        SqsAsyncClient client = clients.computeIfAbsent("", ignored -> createSqsClient(oc));
        clientsByChannel.put(oc.getChannel(), client);

        try {
            SqsOutgoingChannel channel = new SqsOutgoingChannel(
                    new SqsClientHolder<>(client, oc, jsonMapping, new TargetResolver()));
            outgoingChannels.add(channel);
            return channel.getSubscriber();
        } catch (SqsException e) {
            throw ex.illegalStateUnableToBuildConsumer(e);
        }
    }

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        //        incomingChannels.forEach(PulsarIncomingChannel::close);
        outgoingChannels.forEach(SqsOutgoingChannel::close);
        for (SqsAsyncClient client : clients.values()) {
            try {
                client.close();
            } catch (SqsException e) {
                log.unableToCloseClient(e);
            }
        }
        //        incomingChannels.clear();
        outgoingChannels.clear();
        clients.clear();
        clientsByChannel.clear();
    }
}
