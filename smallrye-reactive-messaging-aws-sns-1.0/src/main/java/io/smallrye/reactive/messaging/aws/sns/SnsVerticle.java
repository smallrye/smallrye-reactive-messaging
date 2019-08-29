package io.smallrye.reactive.messaging.aws.sns;

import java.io.ByteArrayInputStream;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import com.amazonaws.services.sns.message.DefaultSnsMessageHandler;
import com.amazonaws.services.sns.message.SnsMessageManager;
import com.amazonaws.services.sns.message.SnsNotification;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheSdkHttpService;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.CreateTopicResponse;
import software.amazon.awssdk.services.sns.model.ListSubscriptionsByTopicRequest;
import software.amazon.awssdk.services.sns.model.ListSubscriptionsByTopicResponse;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sns.model.SubscribeResponse;
import software.amazon.awssdk.services.sns.model.Subscription;

public class SnsVerticle extends AbstractVerticle {

    private final String topicName;
    private final String endpoint;
    private final int port;
    private static final Logger LOG = LoggerFactory.getLogger(SnsVerticle.class);
    private SnsMessageManager messageManager = new SnsMessageManager();
    private BlockingQueue<SnsMessage<String>> msgQ = new LinkedBlockingDeque<>();

    public SnsVerticle(String endpoint, String topicName, int port) {
        this.topicName = topicName;
        this.endpoint = endpoint;
        this.port = port;
    }

    @Override
    public void start(Future<Void> startFuture) {

        Router router = Router.router(vertx);
        router.route()
                .handler(BodyHandler.create())
                .method(HttpMethod.POST);
        router.post(String.format("/sns/%s", topicName)).handler(this::receiveSnsMsg);

        ApacheSdkHttpService apacheSdkHttpService = new ApacheSdkHttpService();
        SdkHttpClient apacheHttpClient = apacheSdkHttpService.createHttpClientBuilder().build();

        try (SnsClient snsClient = SnsClient.builder().httpClient(apacheHttpClient).build()) {
            CreateTopicRequest topicCreationRequest = CreateTopicRequest.builder().name(topicName).build();
            CreateTopicResponse topicCreationResponse = snsClient.createTopic(topicCreationRequest);
            String topicArn = topicCreationResponse.topicArn();
            String topicEndpoint = String.format("%s/sns/%s", endpoint, topicName);
            LOG.info(String.format("Topic ARN is %s, Endpoint is %s", topicArn, topicEndpoint));

            Optional<Subscription> subscription = doesSubscriptionExist(snsClient, topicArn);
            if (!subscription.isPresent()) {
                SubscribeResponse response = snsClient.subscribe(SubscribeRequest
                        .builder()
                        .topicArn(topicArn)
                        .endpoint(topicEndpoint)
                        .protocol("http")
                        .build());
                LOG.info(String.format("Topic Subscribtion response %s", response.subscriptionArn()));
            }
        }

        vertx.createHttpServer().requestHandler(router).listen(port);
    }

    private void receiveSnsMsg(RoutingContext routingContext) {

        JsonObject snsNotification = routingContext.getBodyAsJson();
        LOG.info("Message received ...");

        messageManager.handleMessage(new ByteArrayInputStream(snsNotification.toBuffer().getBytes()),
                new DefaultSnsMessageHandler() {

                    @Override
                    public void handle(SnsNotification snsNotification) {
                        LOG.trace("Handling new message ...");
                        SnsMessage<String> snsMessage = new SnsMessage<String>(snsNotification);
                        msgQ.add(snsMessage);
                        LOG.trace("New message has been added to Q");
                    }
                });
        routingContext.response().setStatusCode(200).end();
    }

    public SnsMessage<String> pollMsg() throws InterruptedException {
        LOG.trace("Pulling message.");
        return msgQ.take();
    }

    /**
     * createTopic is idempotent but subscribe is not so make sure we aren't already subscribed first.
     *
     * @param sns SNS client.
     * @param topicArn ARN of SNS topic.
     * @return True if the {@link #ENDPOINT} is already subscribed to the topic, false otherwise.
     */
    private Optional<Subscription> doesSubscriptionExist(SnsClient sns, String topicArn) {
        String nextToken;
        String fullEndpoint = String.format("%s/sns/%s", endpoint, topicName);
        do {
            ListSubscriptionsByTopicResponse result = sns.listSubscriptionsByTopic(
                    ListSubscriptionsByTopicRequest.builder().topicArn(topicArn).build());
            nextToken = result.nextToken();
            if (result.subscriptions().stream().anyMatch(s -> s.endpoint().equals(fullEndpoint))) {
                return result.subscriptions()
                        .stream()
                        .filter(s -> s.endpoint().equals(fullEndpoint))
                        .findFirst();
            }
        } while (nextToken != null);
        return Optional.empty();
    }
}
