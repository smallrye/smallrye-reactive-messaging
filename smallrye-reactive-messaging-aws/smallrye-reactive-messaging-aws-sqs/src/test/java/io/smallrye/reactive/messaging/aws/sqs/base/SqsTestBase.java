package io.smallrye.reactive.messaging.aws.sqs.base;

import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.awssdk.services.sqs.model.QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.logging.Logger;
import org.jboss.weld.environment.se.Weld;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.wait.strategy.ShellStrategy;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.smallrye.reactive.messaging.aws.base.JbossLogConsumer;
import io.smallrye.reactive.messaging.aws.base.WeldTestBase;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnector;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

@Testcontainers
public class SqsTestBase extends WeldTestBase {

    protected static final LocalStackContainer LOCAL_STACK_CONTAINER = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:latest"))
            .withServices(
                    LocalStackContainer.Service.STS, LocalStackContainer.Service.SQS)
            .withEnv("SKIP_SSL_CERT_DOWNLOAD", "1")
            .withEnv("DISABLE_EVENTS", "1")
            .withEnv("DNS_ADDRESS", "0")
            .withEnv("DEBUG", "1")
            .withEnv("LS_LOG", "info")
            // Due to latest changes the container is in rdy state too early, which can create connection
            // issues. So we wait until it is really rdy.
            .waitingFor(new ShellStrategy().withCommand("awslocal sqs list-queues"));

    static {
        // https://java.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
        // Ryuk will stop it.
        LOCAL_STACK_CONTAINER.start();
    }

    private static SqsAsyncClient CLIENT;
    protected static final String QUEUE_NAME = "test";
    protected static final String FIFO_QUEUE_NAME = "test.fifo";

    @BeforeAll
    static void baseBeforeAll() throws ExecutionException, InterruptedException {
        LOCAL_STACK_CONTAINER.followOutput(new JbossLogConsumer(Logger.getLogger(SqsTestBase.class)));

        CLIENT = SqsAsyncClient.builder()
                .endpointOverride(LOCAL_STACK_CONTAINER.getEndpointOverride(LocalStackContainer.Service.SQS))
                .region(Region.of(LOCAL_STACK_CONTAINER.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build();
        CLIENT.createQueue(CreateQueueRequest.builder().queueName(QUEUE_NAME).build()).get();
        CLIENT.createQueue(CreateQueueRequest.builder().queueName(FIFO_QUEUE_NAME)
                .attributes(Map.of(QueueAttributeName.FIFO_QUEUE, "true")).build()).get();
    }

    @BeforeEach
    void baseBeforeEach() throws ExecutionException, InterruptedException {
        getQueueUrl(QUEUE_NAME)
                .thenCompose(queueUrl -> CLIENT.purgeQueue(
                        PurgeQueueRequest.builder().queueUrl(queueUrl).build()))
                .thenCompose(ignore -> getQueueUrl(FIFO_QUEUE_NAME)
                        .thenCompose(queueUrl -> CLIENT.purgeQueue(
                                PurgeQueueRequest.builder().queueUrl(queueUrl).build())))
                .get();
    }

    @Override
    protected void registerBeanClasses(final Weld weld) {
        weld.addBeanClass(SqsConnector.class);
    }

    @Override
    protected void close() {
        getBeanManager().createInstance()
                .select(SqsConnector.class, ConnectorLiteral.of(SqsConnector.CONNECTOR_NAME))
                .get().terminate(null);
    }

    protected MapBasedConfig getOutgoingConfig() {
        return new MapBasedConfig()
                .with("mp.messaging.outgoing.test.endpoint-override",
                        LOCAL_STACK_CONTAINER.getEndpointOverride(LocalStackContainer.Service.SQS))
                .with("mp.messaging.outgoing.test.region", LOCAL_STACK_CONTAINER.getRegion())
                .with("mp.messaging.outgoing.test.connector", SqsConnector.CONNECTOR_NAME);
    }

    protected MapBasedConfig getIncomingConfig() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.test.endpoint-override",
                        LOCAL_STACK_CONTAINER.getEndpointOverride(LocalStackContainer.Service.SQS))
                .with("mp.messaging.incoming.test.region", LOCAL_STACK_CONTAINER.getRegion())
                .with("mp.messaging.incoming.test.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.test.wait-time-seconds", 2);
    }

    private CompletableFuture<String> getQueueUrl(String queueName) {
        return CLIENT.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build())
                .thenApply(GetQueueUrlResponse::queueUrl);
    }

    protected void sendMessage(String message) {
        sendMessage(QUEUE_NAME, message);
    }

    protected void sendMessage(String queueName, String message) {
        try {
            getQueueUrl(queueName).thenCompose(queueUrl -> CLIENT.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(message).build())).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    protected List<Message> receiveMessages() {
        return receiveMessages(QUEUE_NAME);
    }

    protected List<Message> receiveMessages(String queueName) {
        try {
            final ReceiveMessageResponse response = getQueueUrl(queueName).thenCompose(
                    queueUrl -> CLIENT.receiveMessage(b -> b
                            .queueUrl(queueUrl)
                            .messageAttributeNames("All")
                            .attributeNames(QueueAttributeName.ALL)
                            .waitTimeSeconds(10)
                            .build()))
                    .get();

            if (!response.hasMessages()) {
                return Collections.emptyList();
            }

            return response.messages();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    protected String getQueueAttribute(String queueName, QueueAttributeName attributeName) {
        try {
            return getQueueUrl(queueName).thenCompose(queueUrl -> CLIENT.getQueueAttributes(GetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributeNames(attributeName)
                    .build())).get().attributes().get(attributeName);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    protected Map<QueueAttributeName, String> getQueueAttributes(String queueName, QueueAttributeName... attributeNames) {
        try {
            return getQueueUrl(queueName).thenCompose(queueUrl -> CLIENT.getQueueAttributes(GetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributeNames(attributeNames)
                    .build())).get().attributes();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    protected void verifyNoInvisibleMessages() {
        verifyInvisibleMessages(0);
    }

    protected void verifyInvisibleMessages(int number) {
        String msgsNotVisibile = getQueueAttribute(QUEUE_NAME, APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE);
        assertThat(msgsNotVisibile).isEqualTo(String.valueOf(number));
    }
}
