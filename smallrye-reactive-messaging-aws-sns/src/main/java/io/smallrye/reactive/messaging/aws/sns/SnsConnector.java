package io.smallrye.reactive.messaging.aws.sns;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.CreateTopicResponse;
import software.amazon.awssdk.services.sns.model.PublishRequest;

/**
 * Implement incoming/outgoing connection factories for SNS reactive messaging.
 *
 * @author iabughosh
 */
@ApplicationScoped
@Connector(SnsConnector.CONNECTOR_NAME)
public class SnsConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnsConnector.class);
    static final String CONNECTOR_NAME = "smallrye-aws-sns";

    @Inject
    @ConfigProperty(name = "sns-app-host")
    Optional<String> appHost;

    @Inject
    @ConfigProperty(name = "sns-url")
    Optional<String> snsUrl;

    @Inject
    @ConfigProperty(name = "mock-sns-topics")
    Optional<Boolean> mockSnsTopic;

    @Inject
    Instance<Vertx> instanceOfVertx;

    private boolean internalVertxInstance = false;

    private Vertx vertx;
    private ExecutorService executor;
    private String sinkTopic;
    private String mockSinkUrl;

    @PostConstruct
    public void initConnector() {
        //Initialize vertx and threadExecutor
        LOGGER.info("Initializing Connector");
        if (instanceOfVertx != null && instanceOfVertx.isUnsatisfied()) {
            internalVertxInstance = true;
            this.vertx = Vertx.vertx();
        } else if (instanceOfVertx != null) {
            this.vertx = instanceOfVertx.get();
        }
        executor = Executors.newSingleThreadExecutor();
    }

    /**
     * Its being invoked before bean being destroyed.
     */
    @PreDestroy
    public void preDestroy() {
        if (internalVertxInstance) {
            Optional.ofNullable(vertx).ifPresent(Vertx::close);
        }
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        sinkTopic = getTopicName(config);
        mockSinkUrl = getFakeSnsURL(config);
        return ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(this::send)
                .onError(t -> LOGGER.error("Error while sending the message to SNS topic {}", sinkTopic, t))
                .ignore();
    }

    /**
     * Send message to the SNS Topic.
     *
     * @param message Message to be sent, must not be {@code null}
     * @return the CompletionStage of sending message.
     */
    private CompletionStage<Message<?>> send(Message<?> message) {
        SnsClientConfig clientCfg = getSnsClientConfig(
                mockSinkUrl != null && !mockSinkUrl.trim().isEmpty() ? mockSinkUrl : getSnsURL());
        SnsAsyncClient client = SnsClientManager.get().getAsyncClient(clientCfg);

        CreateTopicRequest topicCreationRequest = CreateTopicRequest.builder().name(sinkTopic).build();
        return client.createTopic(topicCreationRequest)
                .thenApply(CreateTopicResponse::topicArn)
                .thenCompose(arn -> client.publish(PublishRequest
                        .builder()
                        .topicArn(arn)
                        .message((String) message.getPayload())
                        .build()))
                .thenApply(resp -> {
                    LOGGER.info("Message sent successfully with id {}", resp.messageId());
                    return message;
                });
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        String topicName = getTopicName(config);
        Integer port = config.getOptionalValue("port", Integer.class).orElse(8080);
        boolean broadcast = config.getOptionalValue("broadcast", Boolean.class).orElse(false);
        SnsVerticle snsVerticle = new SnsVerticle(getAppHost(), topicName, port, mockSnsTopic.orElse(true),
                getSnsURL());
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
                        // Then poll itemss
                        .onItem().produceMulti(x -> {
                            return Uni.createFrom().item(() -> {
                                // We get a request, block until we get a msg and emit it.
                                try {
                                    LOGGER.trace("Polling message");
                                    return snsVerticle.pollMsg();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    return null; // Signal the end of stream
                                }
                            })
                                    .subscribeOn(executor)
                                    .repeat().until(Objects::isNull);
                        }).concatenate();

        if (broadcast) {
            multi = multi.broadcast().toAllSubscribers();
        }
        return ReactiveStreams.fromPublisher(multi);
    }

    /**
     * Retrieve topic name and throws exception if it is not configured.
     *
     * @param config MicroProfile config instance.
     * @return topic name.
     */
    private String getTopicName(Config config) {
        return config.getOptionalValue("channel", String.class)
                .orElseGet(
                        () -> config.getOptionalValue("topic", String.class)
                                .orElseThrow(() -> new IllegalArgumentException("channel/topic must be set")));
    }

    /**
     * Retrieve Fake SNS URL for unit testing only.
     *
     * @param config MicroProfile config instance.
     * @return Fake SNS URL.
     */
    private String getFakeSnsURL(Config config) {
        return config.getOptionalValue("sns-url", String.class).orElse("");
    }

    /**
     * Retrieve App URL and throws exception if it is not configured.
     *
     * @return application URL accessible by AWS SNS.
     */
    private String getAppHost() {
        return appHost.orElseThrow(() -> new IllegalArgumentException("App URL must be set"));
    }

    /**
     * Retrieve SNS URL and throws exception if it is not configured.
     *
     * @return mock SNS URL.
     */
    private String getSnsURL() {
        return snsUrl.orElseThrow(() -> new IllegalArgumentException("SNS URL must be set"));
    }

    /**
     * Get SNS client config
     *
     * @param host sns url
     * @return Client config object for obtaining SnsClient
     */
    private SnsClientConfig getSnsClientConfig(String host) {
        boolean mockSns = mockSnsTopic == null ? true : mockSnsTopic.orElse(true);
        return new SnsClientConfig(host, mockSns);
    }
}
