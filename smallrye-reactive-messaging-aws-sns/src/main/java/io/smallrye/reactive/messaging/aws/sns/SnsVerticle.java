package io.smallrye.reactive.messaging.aws.sns;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.http.HttpStatus;

import com.amazonaws.services.sns.message.DefaultSnsMessageHandler;
import com.amazonaws.services.sns.message.SnsMessageManager;
import com.amazonaws.services.sns.message.SnsNotification;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.CreateTopicResponse;
import software.amazon.awssdk.services.sns.model.ListSubscriptionsByTopicRequest;
import software.amazon.awssdk.services.sns.model.ListSubscriptionsByTopicResponse;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sns.model.SubscribeResponse;
import software.amazon.awssdk.services.sns.model.Subscription;

/**
 * Vert.x verticle that handles subscription to SNS topic and receive notifications from topic.
 * 
 * @author iabughosh
 * @version 1.0.4
 *
 */
public class SnsVerticle extends AbstractVerticle {

    private final String topicName;
    private final String endpoint;
    private final int port;
    private final boolean mockSns;
    private final String snsUrl;
    private static final Logger LOG = LoggerFactory.getLogger(SnsVerticle.class);
    private SnsMessageManager messageManager = new SnsMessageManager();
    private BlockingQueue<SnsMessage<String>> msgQ = new LinkedBlockingDeque<>();

    /**
     * Parameterized constructor.
     * 
     * @param endpoint Endpoint url.
     * @param topicName SNS topic name.
     * @param port listening port for this verticle.
     * @param true if it is mock/non-sns topic.
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
        router.post(String.format("/sns/%s", topicName)).handler(this::receiveSnsMsg);

        SnsClientConfig clientCfg = new SnsClientConfig(snsUrl, mockSns);
        try (SnsAsyncClient snsClient = SnsClientManager.get().getAsyncClient(clientCfg)) {
            CreateTopicRequest topicCreationRequest = CreateTopicRequest.builder().name(topicName).build();
            CompletableFuture<CreateTopicResponse> topicCreationResponse = snsClient.createTopic(topicCreationRequest);
            String topicArn = topicCreationResponse.get().topicArn();
            String topicEndpoint = mockSns ? String.format("%s:%d/sns/%s", endpoint, port, topicName)
                    : String.format("%s/sns/%s", endpoint, topicName);
            LOG.info(String.format("Topic ARN is %s, Endpoint is %s", topicArn, topicEndpoint));

            Optional<Subscription> subscription = doesSubscriptionExist(snsClient, topicArn);
            if (!subscription.isPresent()) {
                CompletableFuture<SubscribeResponse> response = snsClient.subscribe(SubscribeRequest
                        .builder()
                        .topicArn(topicArn)
                        .endpoint(topicEndpoint)
                        .protocol("http")
                        .build());
                LOG.info(String.format("Topic Subscribtion response %s", response.get().subscriptionArn()));
            }
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("An error occured while starting SNS Verticle", e);
            Thread.currentThread().interrupt();
        }

        vertx.createHttpServer().requestHandler(router)
                .listen(port, new Handler<AsyncResult<HttpServer>>() {

                    @Override
                    public void handle(AsyncResult<HttpServer> event) {
                        if (event.succeeded()) {
                            startFuture.complete();
                        } else {
                            startFuture.fail("Unable to create vertx http server");
                        }
                    }
                });
    }

    /**
     * Handle messages from SNS topic.
     * 
     * @param routingContext vert.x Router routingContext.
     */
    private void receiveSnsMsg(RoutingContext routingContext) {

        JsonObject snsNotification = routingContext.getBodyAsJson();
        LOG.info("Message received ... ");

        if (mockSns) {
            //In case of fake SNS. it will receive message without full AWS SNS attributes
            //so messageManager will not do its full functionality and it will not work.
            //In case of test/fake SNS will receive message and add it directly to msgQ and return success.
            SnsMessage<String> snsMessage = new SnsMessage<>(snsNotification.getString("Message"));
            msgQ.add(snsMessage);
            routingContext.response().setStatusCode(HttpStatus.SC_OK).end();
            return;
        }
        messageManager.handleMessage(new ByteArrayInputStream(snsNotification.toBuffer().getBytes()),
                new DefaultSnsMessageHandler() {

                    @Override
                    public void handle(SnsNotification snsNotification) {
                        LOG.info("Handling new message ...");
                        SnsMessage<String> snsMessage = new SnsMessage<String>(snsNotification);
                        msgQ.add(snsMessage);
                        LOG.trace("New message has been added to Q");
                    }
                });
        routingContext.response().setStatusCode(HttpStatus.SC_OK).end();
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
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private Optional<Subscription> doesSubscriptionExist(SnsAsyncClient sns, String topicArn)
            throws InterruptedException, ExecutionException {
        String nextToken;
        String fullEndpoint = String.format("%s/sns/%s", endpoint, topicName);
        do {
            CompletableFuture<ListSubscriptionsByTopicResponse> result = sns.listSubscriptionsByTopic(
                    ListSubscriptionsByTopicRequest.builder().topicArn(topicArn).build());
            nextToken = result.get().nextToken();
            List<Subscription> currentSubscriptions = result.get().subscriptions();
            if (currentSubscriptions.stream().anyMatch(s -> s.endpoint().equals(fullEndpoint))) {
                return currentSubscriptions
                        .stream()
                        .filter(s -> s.endpoint().equals(fullEndpoint))
                        .findFirst();
            }
        } while (nextToken != null);
        return Optional.empty();
    }
}
