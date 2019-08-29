package io.smallrye.reactive.messaging.aws.sns;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sns.message.DefaultSnsMessageHandler;
import com.amazonaws.services.sns.message.SnsMessageManager;
import com.amazonaws.services.sns.message.SnsNotification;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.*;

/**
 * Vert.x verticle that handles subscription to SNS topic and receive notifications from topic.
 *
 * @author iabughosh
 */
public class SnsVerticle extends AbstractVerticle {

    private final String topicName;
    private final String endpoint;
    private final int port;
    private final boolean mockSns;
    private final String snsUrl;
    private static final Logger LOG = LoggerFactory.getLogger(SnsVerticle.class);
    private SnsMessageManager messageManager = new SnsMessageManager();
    private BlockingQueue<SnsMessage> msgQ = new LinkedBlockingDeque<>();
    private String arn;
    private String topicEndpoint;

    /**
     * Parameterized constructor.
     *
     * @param endpoint Endpoint url.
     * @param topicName SNS topic name.
     * @param port listening port for this verticle.
     * @param mockSns {@code true} if it is mock/non-sns topic.
     * @param snsUrl the SNS url
     */
    public SnsVerticle(String endpoint, String topicName, int port, boolean mockSns, String snsUrl) {
        this.topicName = topicName;
        this.endpoint = endpoint;
        this.port = port;
        this.mockSns = mockSns;
        this.snsUrl = snsUrl;
    }

    @Override
    public void start(Promise<Void> startFuture) {
        Router router = Router.router(vertx);
        router.route()
                .handler(BodyHandler.create())
                .method(HttpMethod.POST);
        router.head().handler(rc -> rc.response().setStatusCode(204).end());
        router.post(String.format("/sns/%s", topicName)).handler(this::receiveSnsMsg);

        SnsClientConfig clientCfg = new SnsClientConfig(snsUrl, mockSns);
        SnsAsyncClient snsClient = SnsClientManager.get().getAsyncClient(clientCfg);
        CreateTopicRequest topicCreationRequest = CreateTopicRequest.builder().name(topicName).build();
        CompletableFuture<CreateTopicResponse> topicCreationResponse = snsClient.createTopic(topicCreationRequest);
        topicCreationResponse
                .thenCompose(res -> {
                    arn = res.topicArn();
                    topicEndpoint = mockSns ? String.format("%s:%d/sns/%s", endpoint, port, topicName)
                            : String.format("%s/sns/%s", endpoint, topicName);
                    LOG.info(String.format("Topic ARN is %s, Endpoint is %s", arn, topicEndpoint));
                    return isSubscribed(snsClient, arn);
                })
                .thenCompose(subscribed -> {
                    if (!subscribed) {
                        LOG.info("Subscribing to topic {} with arn {}", topicEndpoint, arn);
                        return snsClient.subscribe(SubscribeRequest
                                .builder()
                                .topicArn(arn)
                                .endpoint(topicEndpoint)
                                .protocol("http")
                                .build()).thenApply(x -> null);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                }).thenAccept(x -> vertx.createHttpServer()
                        .requestHandler(router)
                        .listen(port, ar -> startFuture.handle(ar.mapEmpty())));
    }

    /**
     * Handle messages from SNS topic.
     *
     * @param routingContext vert.x Router routingContext.
     */
    private void receiveSnsMsg(RoutingContext routingContext) {
        JsonObject snsNotification = routingContext.getBodyAsJson();
        LOG.info("Message received from SNS");
        if (mockSns) {
            //In case of fake SNS. it will receive message without full AWS SNS attributes
            //so messageManager will not do its full functionality and it will not work.
            //In case of test/fake SNS will receive message and add it directly to msgQ and return success.
            SnsMessage snsMessage = new SnsMessage(snsNotification.getString("Message"));
            msgQ.add(snsMessage);
            routingContext.response().setStatusCode(200).end();
            return;
        }
        messageManager.handleMessage(new ByteArrayInputStream(snsNotification.toBuffer().getBytes()),
                new DefaultSnsMessageHandler() {

                    @Override
                    public void handle(SnsNotification notification) {
                        SnsMessage snsMessage = new SnsMessage(notification);
                        msgQ.add(snsMessage);
                        LOG.trace("New message has been added to Q");
                    }
                });
        routingContext.response().setStatusCode(200).end();
    }

    SnsMessage pollMsg() throws InterruptedException {
        LOG.trace("Pulling message.");
        return msgQ.take();
    }

    private CompletionStage<Boolean> isSubscribed(SnsAsyncClient sns, String arn) {
        String fullEndpoint = String.format("%s/sns/%s", endpoint, topicName);
        CompletableFuture<ListSubscriptionsByTopicResponse> result = sns.listSubscriptionsByTopic(
                ListSubscriptionsByTopicRequest.builder().topicArn(arn).build());
        return result.thenApply(list -> {
            List<Subscription> subscriptions = list.subscriptions();
            return subscriptions.stream().anyMatch(s -> s.endpoint().equalsIgnoreCase(fullEndpoint));
        });
    }
}
