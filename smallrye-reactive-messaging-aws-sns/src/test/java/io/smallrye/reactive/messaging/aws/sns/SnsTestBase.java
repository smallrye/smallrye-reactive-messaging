package io.smallrye.reactive.messaging.aws.sns;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

@Testcontainers
public class SnsTestBase extends WeldTestBase {

    private static final DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:4.2.0");

    @Container
    public static LocalStackContainer localstack = new LocalStackContainer(localstackImage)
            .withServices(LocalStackContainer.Service.SQS, LocalStackContainer.Service.SNS);

    private static SnsAsyncClient snsAsyncClient;
    private static SqsAsyncClient sqsClient;

    protected String topic;
    protected String topicArn;

    @BeforeEach
    void setupLocalstack(final TestInfo testInfo) {
        System.setProperty("aws.accessKeyId", localstack.getAccessKey());
        System.setProperty("aws.secretAccessKey", localstack.getSecretKey());
        final var mn = testInfo.getTestMethod().map(Method::getName).orElse(UUID.randomUUID().toString());
        final var topicName = mn + "-" + UUID.randomUUID().getMostSignificantBits();
        createTopic(topicName);
    }

    protected void createTopic(final String topicName) {
        // limit the topic name to max 80 characters. Otherwise, localstack will complain.
        topic = topicName.substring(0, Math.min(80, topicName.length()));
        // For our tests we need to guess the topic arn so that we can set it in the config.
        // arn:aws:sns:us-east-2:123456789012:MyTopic
        topicArn = guessTopicName(topic);
    }

    public String guessTopicName(String name) {
        return String.format("arn:aws:sns:%s:000000000000:%s", localstack.getRegion(), name);
    }

    @AfterEach
    void stopContainer() {
        if (container != null) {
            final var connector = getBeanManager().createInstance()
                    .select(SnsConnector.class, ConnectorLiteral.of(SnsConnector.CONNECTOR_NAME))
                    .get();
            connector.terminate(null);
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @AfterAll
    static void stopClient() {
        if (snsAsyncClient != null) {
            snsAsyncClient.close();
            snsAsyncClient = null;
        }
        if (sqsClient != null) {
            sqsClient.close();
            sqsClient = null;
        }
    }

    public synchronized SnsAsyncClient getSnsClient() {
        if (snsAsyncClient == null) {
            snsAsyncClient = SnsAsyncClient.builder().endpointOverride(localstack.getEndpoint())
                    .credentialsProvider(SystemPropertyCredentialsProvider.create())
                    .region(Region.of(localstack.getRegion()))
                    .build();
        }
        return snsAsyncClient;
    }

    public synchronized SqsAsyncClient getSqsClient() {
        if (sqsClient == null) {
            sqsClient = SqsAsyncClient.builder().endpointOverride(localstack.getEndpoint())
                    .credentialsProvider(SystemPropertyCredentialsProvider.create())
                    .region(Region.of(localstack.getRegion()))
                    .build();
        }
        return sqsClient;
    }

    public List<Message> receiveAndDeleteMessages(String queueUrl, int numberOfMessages, Duration timeout) {
        Instant timeoutTs = Instant.now().plus(timeout);
        return receiveAndDeleteMessages(queueUrl,
                messages -> messages.size() >= numberOfMessages || Instant.now().isAfter(timeoutTs));
    }

    public List<Message> receiveAndDeleteMessages(String queueUrl, Predicate<List<Message>> stopCondition) {
        return receiveAndDeleteMessages(queueUrl, r -> {
        }, stopCondition);
    }

    public List<Message> receiveAndDeleteMessages(String queueUrl,
            Consumer<ReceiveMessageRequest.Builder> receiveMessageRequest,
            int numberOfMessages, Duration timeout) {
        Instant timeoutTs = Instant.now().plus(timeout);
        return receiveAndDeleteMessages(queueUrl, receiveMessageRequest,
                messages -> messages.size() >= numberOfMessages || Instant.now().isAfter(timeoutTs));
    }

    public List<Message> receiveAndDeleteMessages(
            String queueUrl,
            Consumer<ReceiveMessageRequest.Builder> receiveMessageRequest,
            Predicate<List<Message>> stopCondition) {
        var sqsClient = getSqsClient();
        var received = new CopyOnWriteArrayList<Message>();
        while (!stopCondition.test(received)) {
            try {
                ReceiveMessageResponse receiveMessageResponse = sqsClient
                        .receiveMessage(r -> receiveMessageRequest
                                .accept(r.queueUrl(queueUrl).waitTimeSeconds(1).maxNumberOfMessages(10)))
                        .get();
                if (receiveMessageResponse.hasMessages()) {
                    List<DeleteMessageBatchRequestEntry> entries = new ArrayList<>();
                    for (Message message : receiveMessageResponse.messages()) {
                        entries.add(DeleteMessageBatchRequestEntry.builder()
                                .id(message.messageId())
                                .receiptHandle(message.receiptHandle())
                                .build());
                    }
                    sqsClient.deleteMessageBatch(DeleteMessageBatchRequest.builder()
                            .queueUrl(queueUrl)
                            .entries(entries)
                            .build())
                            .get();
                    received.addAll(receiveMessageResponse.messages());
                }
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return received;
    }

    public TopicQueueSubscription createTopicWithFifoQueueSubscription(String name) {
        return createTopicWithQueueSubscription(name,
                b -> b.name(name + ".fifo")
                        .attributes(Map.of(
                                "FifoTopic", "true",
                                "FifoThroughputScope", "MessageGroup")),
                b -> b.queueName(name + ".fifo")
                        .attributes(Map.of(
                                QueueAttributeName.FIFO_QUEUE, "true",
                                QueueAttributeName.DEDUPLICATION_SCOPE, "messageGroup")));
    }

    public TopicQueueSubscription createTopicWithQueueSubscription(String name) {
        return createTopicWithQueueSubscription(name, b -> {
        }, b -> {
        });
    }

    public TopicQueueSubscription createTopicWithQueueSubscription(String name,
            Consumer<CreateTopicRequest.Builder> createTopicRequestConsumer,
            Consumer<CreateQueueRequest.Builder> createQueueRequestConsumer) {
        try {
            final var topicArn = getSnsClient().createTopic(b -> createTopicRequestConsumer
                    .accept(b.name(name))).get().topicArn();
            final var queueUrl = getSqsClient().createQueue(b -> createQueueRequestConsumer
                    .accept(b.queueName(name))).get().queueUrl();
            final var queueArn = getSqsClient().getQueueAttributes(b -> b.queueUrl(queueUrl)
                    .attributeNames(QueueAttributeName.QUEUE_ARN))
                    .get().attributes().get(QueueAttributeName.QUEUE_ARN);

            getSnsClient().subscribe(b -> b.topicArn(topicArn)
                    .attributes(Map.of("RawMessageDelivery", "true"))
                    .protocol("sqs")
                    .endpoint(queueArn)).get();

            return new TopicQueueSubscription(topicArn, queueUrl);

        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public record TopicQueueSubscription(String topicArn, String queueUrl) {
    }

    public HealthCenter getHealth() {
        if (container == null) {
            throw new IllegalStateException("Application not started");
        }
        return container.getBeanManager().createInstance().select(HealthCenter.class).get();
    }

    public boolean isAlive() {
        return getHealth().getLiveness().isOk();
    }
}
