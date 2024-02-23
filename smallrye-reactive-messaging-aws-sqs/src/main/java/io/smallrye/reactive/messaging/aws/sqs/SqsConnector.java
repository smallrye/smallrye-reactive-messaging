package io.smallrye.reactive.messaging.aws.sqs;

import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(SqsConnector.CONNECTOR_NAME)
@ConnectorAttribute(name = "queue", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The name of the SQS queue")
@ConnectorAttribute(name = "credentialsProvider", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The credential provider to be used in the client")
@ConnectorAttribute(name = "waitTimeSeconds", type = "int", direction = ConnectorAttribute.Direction.INCOMING, description = "The maximum amount of time in seconds to wait for messages to be received")
@ConnectorAttribute(name = "maxNumberOfMessages", type = "int", direction = ConnectorAttribute.Direction.INCOMING, description = "The maximum number of messages to receive")
public class SqsConnector implements InboundConnector, OutboundConnector {

    @Inject
    private SqsManager sqsManager;

    @Inject
    ExecutionHolder executionHolder;

    Vertx vertx;

    private static final List<SqsInboundChannel> INBOUND_CHANNELS = new CopyOnWriteArrayList<>();
    private static final List<SqsOutboundChannel> OUTBOUND_CHANNELS = new CopyOnWriteArrayList<>();
    public static final String CONNECTOR_NAME = "smallrye-sqs";

    @PostConstruct
    void init() {
        this.vertx = executionHolder.vertx();
    }

    @PreDestroy
    void close() {
        INBOUND_CHANNELS.forEach(SqsInboundChannel::close);
        OUTBOUND_CHANNELS.forEach(SqsOutboundChannel::close);
    }

    @Override
    public Publisher<? extends Message<?>> getPublisher(Config config) {
        var sqsConfig = new SqsConfig(config);
        var channel = new SqsInboundChannel(vertx, sqsConfig, sqsManager.getQueueUrl(sqsConfig),
                sqsManager.getClient(sqsConfig));
        INBOUND_CHANNELS.add(channel);
        return channel.getStream();
    }

    @Override
    public Subscriber<? extends Message<?>> getSubscriber(Config config) {
        var sqsConfig = new SqsConfig(config);
        var channel = new SqsOutboundChannel(sqsManager.getClient(sqsConfig),
                sqsManager.getQueueUrl(sqsConfig));
        OUTBOUND_CHANNELS.add(channel);
        return channel.getSubscriber();
    }
}
