package io.smallrye.reactive.messaging.aws.sns;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.*;
import static io.smallrye.reactive.messaging.aws.sns.i18n.SnsLogging.log;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.vertx.core.Vertx;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.CreateTopicResponse;
import software.amazon.awssdk.services.sns.model.PublishRequest;

/**
 * Implement incoming/outgoing connection factories for Amazon Simple Notification Service reactive messaging.
 *
 * @author iabughosh
 */
@ApplicationScoped
@Connector(SnsConnector.CONNECTOR_NAME)

@ConnectorAttribute(name = "topic", description = "The SNS topic. If not set, the channel name is used.", type = "string", direction = INCOMING_AND_OUTGOING)
@ConnectorAttribute(name = "sns-url", description = "Set the Amazon SNS subscription url. It can be set to a local URL for testing.", type = "string", mandatory = true, direction = INCOMING_AND_OUTGOING, alias = "sns-url")
@ConnectorAttribute(name = "port", description = "The SNS Callback port", type = "int", defaultValue = "8080", direction = INCOMING)
@ConnectorAttribute(name = "host", description = "The SNS URL / Endpoint URL", type = "string", mandatory = true, direction = INCOMING)
@ConnectorAttribute(name = "broadcast", description = "Whether the SNS messages are dispatched to multiple subscribers (`@Incoming`)", type = "boolean", defaultValue = "false", direction = INCOMING)
@ConnectorAttribute(name = "mock-sns-topics", description = "Indicates to the connector to use mock/fake topics. _For testing only_", type = "boolean", defaultValue = "false", direction = INCOMING_AND_OUTGOING, alias = "sns-mock-topics")
@ConnectorAttribute(name = "app-url", description = "Configures AWS App public URL. This URL should be accessible by AWS SNS (subscription url). It can be a public URL or an URL accessible by SNS within the same VPC.", type = "string", direction = INCOMING, alias = "sns-app-url")
@ConnectorAttribute(name = "merge", direction = OUTGOING, description = "Whether the connector should allow multiple upstreams", type = "boolean", defaultValue = "false")

public class SnsConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

    static final String CONNECTOR_NAME = "smallrye-aws-sns";

    @Inject
    ExecutionHolder executionHolder;

    private Vertx vertx;
    private ExecutorService executor;

    @PostConstruct
    public void initConnector() {
        //Initialize vertx and threadExecutor
        log.initializingConnector();
        this.vertx = executionHolder.vertx().getDelegate();
        executor = Executors.newSingleThreadExecutor();
    }

    // For Testing
    void setup(ExecutionHolder executionHolder) {
        this.executionHolder = executionHolder;
    }

    /**
     * Its being invoked before bean being destroyed.
     */
    @PreDestroy
    public void preDestroy() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        SnsConnectorOutgoingConfiguration oc = new SnsConnectorOutgoingConfiguration(config);
        String topic = oc.getTopic().orElseGet(oc::getChannel);
        String snsUrl = oc.getSnsUrl();
        boolean mock = oc.getMockSnsTopics();
        return ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(m -> send(m, topic, snsUrl, mock))
                .onError(t -> log.errorSendingToTopic(topic, t))
                .ignore();
    }

    /**
     * Send message to the SNS Topic.
     *
     * @param message Message to be sent, must not be {@code null}
     * @return the CompletionStage of sending message.
     */
    private CompletionStage<Message<?>> send(Message<?> message, String topic, String snsUrl, boolean mock) {
        SnsClientConfig clientConfig = new SnsClientConfig(snsUrl, mock);
        SnsAsyncClient client = SnsClientManager.get().getAsyncClient(clientConfig);

        CreateTopicRequest topicCreationRequest = CreateTopicRequest.builder().name(topic).build();
        return client.createTopic(topicCreationRequest)
                .thenApply(CreateTopicResponse::topicArn)
                .thenCompose(arn -> client.publish(PublishRequest
                        .builder()
                        .topicArn(arn)
                        .message((String) message.getPayload())
                        .build()))
                .thenApply(resp -> {
                    log.successfullySend(resp.messageId());
                    return message;
                });
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        SnsConnectorIncomingConfiguration ic = new SnsConnectorIncomingConfiguration(config);
        String topic = ic.getTopic().orElseGet(ic::getChannel);
        int port = ic.getPort();
        boolean broadcast = ic.getBroadcast();
        String host = ic.getHost();
        String snsUrl = ic.getSnsUrl();
        SnsVerticle snsVerticle = new SnsVerticle(host, topic, port, ic.getMockSnsTopics(), snsUrl);
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        vertx.deployVerticle(snsVerticle, ar -> {
            if (ar.succeeded()) {
                future.complete(true);
            } else {
                future.completeExceptionally(ar.cause());
            }
        });

        Multi<SnsMessage> multi =
                // First wait until the verticle is deployed.
                Multi.createFrom().completionStage(future)
                        // Then poll items
                        .onItem().transformToMultiAndConcatenate(x -> Uni.createFrom().item(() -> {
                            // We get a request, block until we get a msg and emit it.
                            try {
                                log.polling();
                                return snsVerticle.pollMsg();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return null; // Signal the end of stream
                            }
                        })
                                .runSubscriptionOn(executor)
                                .repeat().until(Objects::isNull));

        if (broadcast) {
            multi = multi.broadcast().toAllSubscribers();
        }
        return ReactiveStreams.fromPublisher(multi);
    }
}
