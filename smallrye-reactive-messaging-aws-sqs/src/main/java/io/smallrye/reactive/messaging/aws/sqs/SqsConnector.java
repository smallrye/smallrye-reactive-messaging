package io.smallrye.reactive.messaging.aws.sqs;

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
@ConnectorAttribute(name = "waitTimeSeconds", type = "int", direction = ConnectorAttribute.Direction.INCOMING, description = "The maximum amount of time in seconds to wait for messages to be received")
@ConnectorAttribute(name = "maxNumberOfMessages", type = "int", direction = ConnectorAttribute.Direction.INCOMING, description = "The maximum number of messages to receive")
public class SqsConnector implements InboundConnector, OutboundConnector {

    @Inject
    private SqsManager sqsManager;

    @Inject
    ExecutionHolder executionHolder;

    Vertx vertx;

    public static final String CONNECTOR_NAME = "smallrye-sqs";

    @PostConstruct
    void init() {
        this.vertx = executionHolder.vertx();
    }

    @Override
    public Publisher<? extends Message<?>> getPublisher(Config config) {
        var sqsConfig = new SqsConfig(config);
        return new SqsInboundChannel(vertx, sqsConfig, sqsManager.getQueueUrl(sqsConfig), sqsManager.getClient(sqsConfig))
                .getStream();
    }

    @Override
    public Subscriber<? extends Message<?>> getSubscriber(Config config) {
        var sqsConfig = new SqsConfig(config);
        return new SqsOutboundChannel(sqsManager.getClient(sqsConfig),
                sqsManager.getQueueUrl(sqsConfig)).getSubscriber();
    }
}
