package io.smallrye.reactive.messaging.aws.sns;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.processors.MulticastProcessor;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.Vertx;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.CreateTopicResponse;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

/**
 * Implement incoming/outcoming connection factories for SNS reactive messaging.
 * 
 * @author iabughosh
 * @version 1.0.4
 *
 */
@ApplicationScoped
@Connector(SnsConnector.CONNECTOR_NAME)
public class SnsConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnsConnector.class);
    static final String CONNECTOR_NAME = "smallrye-aws-sns";

    /*
     * Injectable fields
     */
    @Inject
    @ConfigProperty(name = "sns-app-host")
    Optional<String> appHost;
    @Inject
    @ConfigProperty(name = "sns-url")
    Optional<String> snsUrl;
    @Inject
    @ConfigProperty(name = "mock-sns-topics")
    Optional<Boolean> mockSnsTopic;
    /** If there is existing vert.x instance, inject it. */
    @Inject
    Instance<Vertx> instanceOfVertx;
    /*
     * Fields
     */
    /** Control wither close vert.x manually or not */
    private boolean internalVertxInstance = false;

    private Vertx vertx;
    private Scheduler scheduler;
    private String sinkTopic;
    private String mockSinkUrl;

    /**
     * Method being invoked when CDI bean first created.
     */
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
        scheduler = Schedulers.single();
    }

    /**
     * Its being invoked before bean being destroyed.
     */
    @PreDestroy
    public void preDestroy() {
        LOGGER.info("Destroying Connector");
        if (internalVertxInstance) {
            Optional.ofNullable(vertx).ifPresent(Vertx::close);
        }
        Optional.ofNullable(scheduler).ifPresent(Scheduler::shutdown);
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {

        sinkTopic = getTopicName(config);
        mockSinkUrl = getFakeSnsURL(config);
        return ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(this::send)
                .onError(t -> LOGGER.error("Error while subscribing to connector", t)).ignore();
    }

    /**
     * Send message to SNS Topic.
     * 
     * @param msg Message to be sent
     * @return CompletionStage of sending message.
     */
    private CompletionStage<Message<?>> send(Message<?> message) {

        return CompletableFuture.runAsync(() -> {
            SnsClientConfig clientCfg = getSnsClientConfig(
                    mockSinkUrl != null && !mockSinkUrl.trim().isEmpty() ? mockSinkUrl : getSnsURL());
            //send to sns
            try (SnsAsyncClient snsClient = SnsClientManager.get().getAsyncClient(clientCfg)) {
                //Prepare create topic request. if it is already created topicARN will be reutrned.
                CreateTopicRequest topicCreationRequest = CreateTopicRequest.builder().name(sinkTopic).build();
                CompletableFuture<CreateTopicResponse> topicCreationResponse = snsClient.createTopic(topicCreationRequest);
                String topicArn = topicCreationResponse.get().topicArn();
                //Prepare publish message request to SNS topic
                PublishRequest pr = PublishRequest
                        .builder()
                        .topicArn(topicArn)
                        .message((String) message.getPayload())
                        .build();
                CompletableFuture<PublishResponse> response = snsClient.publish(pr);
                LOGGER.info("Message ID {}", response.get().messageId());
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Async call interruption happened !!!", e);
                Thread.currentThread().interrupt();
            }
        }).thenApply(x -> message);
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {

        String topicName = getTopicName(config);
        Integer port = config.getOptionalValue("port", Integer.class).orElse(8080);
        boolean broadcast = config.getOptionalValue("broadcast", Boolean.class).orElse(false);
        int initialDelay = config.getOptionalValue("initDelay", Integer.class).orElse(2000);
        SnsVerticle snsVerticle = new SnsVerticle(getAppHost(), topicName, port, mockSnsTopic.orElse(true), getSnsURL());
        vertx.deployVerticle(snsVerticle);

        Flowable<SnsMessage<String>> flowable = Flowable.fromCallable(() -> {

            SnsMessage<String> snsMsg = null;
            try {
                LOGGER.trace("Polling message");
                //It uses blockingQ.take() method so it will block until there is 
                //an item available. Thats why in repeatWhen it is only 1 millisecond.
                snsMsg = snsVerticle.pollMsg();
                Objects.requireNonNull(snsMsg, "Null message has been consumed from Q");
            } catch (InterruptedException e) {
                throw e;
            }
            return snsMsg;
        }).repeatWhen(o -> o.concatMap(v -> Flowable.timer(1, TimeUnit.MILLISECONDS)))
                .delaySubscription(initialDelay, TimeUnit.MILLISECONDS)
                .subscribeOn(scheduler);

        PublisherBuilder<? extends Message<?>> builder = ReactiveStreams.fromPublisher(flowable);

        return broadcast ? builder.via(MulticastProcessor.create()) : builder;
    }

    /**
     * Retrieve topic name and throws exception if it is not configured.
     * 
     * @param config microprofile config instance.
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
     * @param config microprofile config instance.
     * @return Fake SNS URL.
     */
    private String getFakeSnsURL(Config config) {
        return config.getOptionalValue("sns-url", String.class)
                .orElseGet(() -> "");
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
        //Check null in case of unit test.
        return snsUrl.orElseThrow(() -> new IllegalArgumentException("SNS URL must be set"));
    }

    /**
     * Get SNS client config
     * 
     * @param host sns url
     * @return Client config object for obtaining SnsClient
     */
    private SnsClientConfig getSnsClientConfig(String host) {
        //Check null in case of unit test.
        boolean mockSns = mockSnsTopic == null ? true : mockSnsTopic.orElse(true);
        return new SnsClientConfig(host, mockSns);
    }
}
