package io.smallrye.reactive.messaging.aws.sqs;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
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
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

@Testcontainers
public class SqsTestBase extends WeldTestBase {

    private final static DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:3.1.0");

    @Container
    public static LocalStackContainer localstack = new LocalStackContainer(localstackImage)
            .withServices(LocalStackContainer.Service.SQS);

    private static SqsAsyncClient client;

    protected String queue;

    @BeforeEach
    void setupLocalstack(TestInfo testInfo) {
        System.setProperty("aws.accessKeyId", localstack.getAccessKey());
        System.setProperty("aws.secretAccessKey", localstack.getSecretKey());
        String mn = testInfo.getTestMethod().map(Method::getName).orElse(UUID.randomUUID().toString());
        queue = mn + "-" + UUID.randomUUID().getMostSignificantBits();
    }

    @AfterEach
    public void stopContainer() {
        if (container != null) {
            var connector = getBeanManager().createInstance()
                    .select(SqsConnector.class, ConnectorLiteral.of(SqsConnector.CONNECTOR_NAME))
                    .get();
            connector.terminate(null);
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @AfterAll
    public static void stopClient() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    public String sendMessage(String queueUrl, int numberOfMessages,
            BiConsumer<Integer, SendMessageRequest.Builder> messageProvider) {
        var sqsClient = getSqsClient();
        for (int i = 0; i < numberOfMessages; i++) {
            int j = i;
            try {
                sqsClient.sendMessage(r -> messageProvider.accept(j, r.queueUrl(queueUrl))).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        return queueUrl;
    }

    public String sendMessage(String queueUrl, int numberOfMessages) {
        return sendMessage(queueUrl, numberOfMessages, (i, b) -> b.messageBody("hello"));
    }

    public synchronized SqsAsyncClient getSqsClient() {
        if (client == null) {
            client = SqsAsyncClient.builder().endpointOverride(localstack.getEndpoint())
                    .credentialsProvider(SystemPropertyCredentialsProvider.create())
                    .region(Region.of(localstack.getRegion()))
                    .build();
        }
        return client;
    }

    public List<Message> receiveMessages(String queueUrl,
            Consumer<ReceiveMessageRequest.Builder> receiveMessageRequest,
            Predicate<List<Message>> stopCondition) {
        var sqsClient = getSqsClient();
        var received = new CopyOnWriteArrayList<Message>();
        while (!stopCondition.test(received)) {
            try {
                received.addAll(sqsClient
                        .receiveMessage(r -> receiveMessageRequest.accept(r.queueUrl(queueUrl).maxNumberOfMessages(10)))
                        .get()
                        .messages());
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return received;
    }

    public List<Message> receiveMessages(String queueUrl, Predicate<List<Message>> stopCondition) {
        return receiveMessages(queueUrl, r -> {
        }, stopCondition);
    }

    public List<Message> receiveMessages(String queueUrl, int numberOfMessages, Duration timeout) {
        Instant timeoutTs = Instant.now().plus(timeout);
        return receiveMessages(queueUrl, messages -> messages.size() >= numberOfMessages || Instant.now().isAfter(timeoutTs));
    }

    public List<Message> receiveMessages(String queueUrl, Consumer<ReceiveMessageRequest.Builder> receiveMessageRequest,
            int numberOfMessages, Duration timeout) {
        Instant timeoutTs = Instant.now().plus(timeout);
        return receiveMessages(queueUrl, receiveMessageRequest,
                messages -> messages.size() >= numberOfMessages || Instant.now().isAfter(timeoutTs));
    }

    public String createQueue(String queueName) {
        try {
            return getSqsClient().createQueue(r -> r.queueName(queueName)).get().queueUrl();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public String createQueue(String queueName, Consumer<CreateQueueRequest.Builder> requestConsumer) {
        try {
            return getSqsClient().createQueue(r -> requestConsumer.accept(r.queueName(queueName))).get().queueUrl();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public HealthCenter getHealth() {
        if (container == null) {
            throw new IllegalStateException("Application not started");
        }
        return container.getBeanManager().createInstance().select(HealthCenter.class).get();
    }

    public boolean isStarted() {
        return getHealth().getStartup().isOk();
    }

    public boolean isReady() {
        return getHealth().getReadiness().isOk();
    }

    public boolean isAlive() {
        return getHealth().getLiveness().isOk();
    }
}
