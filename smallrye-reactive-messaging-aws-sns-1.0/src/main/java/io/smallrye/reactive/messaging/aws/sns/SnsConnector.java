package io.smallrye.reactive.messaging.aws.sns;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
import io.reactivex.processors.MulticastProcessor;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.Vertx;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheSdkHttpService;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.CreateTopicResponse;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

@ApplicationScoped
@Connector(SnsConnector.CONNECTOR_NAME)
public class SnsConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnsConnector.class);
    static final String CONNECTOR_NAME = "smallrye-aws-sns";

    @Inject
    @ConfigProperty(name = "sns-app-public-url")
    Optional<String> appURL;
    @Inject
    Instance<Vertx> instanceOfVertx;

    private boolean internalVertxInstance = false;
    private Vertx vertx;
    private ExecutorService threadExecutor;

    @Inject
    public void init() {
        LOGGER.info("Initializing Connector");
        if (instanceOfVertx.isUnsatisfied()) {
            internalVertxInstance = true;
            this.vertx = Vertx.vertx();
        } else {
            this.vertx = instanceOfVertx.get();
        }
        threadExecutor = Executors.newSingleThreadExecutor();
    }

    @PreDestroy
    public void preDestroy() {
        LOGGER.info("Destroying Connector");
        if (internalVertxInstance) {
            Optional.ofNullable(vertx).ifPresent(vertx -> vertx.close());
        }
        Optional.of(threadExecutor).ifPresent(exec -> exec.shutdown());
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {

        String topicName = getTopicName(config);
        return ReactiveStreams.<Message<?>> builder().flatMapCompletionStage(message -> {
            CompletionStage<Message<?>> cs = CompletableFuture.runAsync(() -> {
                //send to sns
                send(message, topicName);
            }, threadExecutor).thenApply(x -> message);
            return cs;
        }).ignore();
    }

    private void send(Message<?> msg, String topicName) {

        try (SnsClient snsClient = openSnsClient()) {
            CreateTopicRequest topicCreationRequest = CreateTopicRequest.builder().name(topicName).build();
            CreateTopicResponse topicCreationResponse = snsClient.createTopic(topicCreationRequest);
            String topicArn = topicCreationResponse.topicArn();

            PublishRequest pr = PublishRequest
                    .builder()
                    .topicArn(topicArn)
                    .message((String) msg.getPayload())
                    .build();
            PublishResponse response = snsClient.publish(pr);
            LOGGER.info("Message ID {}", response.messageId());
        }
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {

        String topicName = getTopicName(config);
        Integer port = config.getValue("port", Integer.class);
        boolean broadcast = config.getOptionalValue("broadcast", Boolean.class).orElse(false);
        int initialDelay = config.getOptionalValue("initDelay", Integer.class).orElse(2000);
        SnsVerticle snsVerticle = new SnsVerticle(getAppURL(), topicName, port);
        vertx.deployVerticle(snsVerticle);

        Flowable<SnsMessage<String>> flowable = Flowable.fromCallable(() -> {

            SnsMessage<String> snsMsg = null;
            try {
                LOGGER.trace("Polling message");
                snsMsg = snsVerticle.pollMsg();
                Objects.requireNonNull(snsMsg, "Null message has been consumed from Q");
            } catch (InterruptedException e) {
                LOGGER.error("Polling interrupted", e);
                throw e;
            }
            return snsMsg;
        }).repeatWhen(o -> o.concatMap(v -> Flowable.timer(1, TimeUnit.MILLISECONDS)))
                .delaySubscription(initialDelay, TimeUnit.MILLISECONDS)
                .subscribeOn(Schedulers.single());

        PublisherBuilder<? extends Message<?>> builder = ReactiveStreams.fromPublisher(flowable);

        return broadcast ? builder.via(MulticastProcessor.create()) : builder;
    }

    private SnsClient openSnsClient() {

        ApacheSdkHttpService apacheSdkHttpService = new ApacheSdkHttpService();
        SdkHttpClient apacheHttpClient = apacheSdkHttpService.createHttpClientBuilder().build();
        return SnsClient.builder().httpClient(apacheHttpClient).build();
    }

    private String getTopicName(Config config) {
        return config.getOptionalValue("address", String.class)
                .orElseGet(
                        () -> config.getOptionalValue("topic-name", String.class)
                                .orElseThrow(() -> new IllegalArgumentException("Address/topic-name must be set")));
    }

    private String getAppURL() {
        return appURL.orElseThrow(() -> new IllegalArgumentException("App URL must be set"));
    }
}
