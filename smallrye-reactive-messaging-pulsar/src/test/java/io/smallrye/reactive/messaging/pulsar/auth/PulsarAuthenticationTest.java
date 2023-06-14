package io.smallrye.reactive.messaging.pulsar.auth;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.auth.AuthenticationBasic;
import org.assertj.core.api.Assertions;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.MountableFile;

import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.base.PulsarContainer;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBaseWithoutExtension;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PulsarAuthenticationTest extends WeldTestBaseWithoutExtension {

    public static final int NUMBER_OF_MESSAGES = 100;

    static PulsarContainer container;

    @BeforeAll
    static void beforeAll() throws PulsarClientException, InterruptedException {
        container = new PulsarContainer()
                .withCopyToContainer(MountableFile.forClasspathResource("htpasswd"), "/pulsar/conf/.htpasswd")
                .withEnv("PULSAR_PREFIX_authenticationEnabled", "true")
                .withEnv("PULSAR_PREFIX_authenticationProviders",
                        "org.apache.pulsar.broker.authentication.AuthenticationProviderBasic")
                .withEnv("PULSAR_PREFIX_basicAuthConf", "file:///pulsar/conf/.htpasswd")
                .withEnv("PULSAR_PREFIX_brokerClientAuthenticationEnabled", "true")
                .withEnv("PULSAR_PREFIX_brokerClientAuthenticationPlugin",
                        "org.apache.pulsar.client.impl.auth.AuthenticationBasic")
                .withEnv("PULSAR_PREFIX_brokerClientAuthenticationParameters",
                        "{\"userId\":\"superuser\",\"password\":\"admin\"}");
        container.start();

        String clusterUrl = container.getPulsarBrokerUrl();
        String serviceHttpUrl = container.getHttpServiceUrl();
        serviceUrl = clusterUrl;
        AuthenticationBasic auth = new AuthenticationBasic();
        auth.configure("{\"userId\":\"superuser\",\"password\":\"admin\"}");
        client = PulsarClient.builder()
                .serviceUrl(clusterUrl)
                .authentication(auth)
                .build();
        admin = PulsarAdmin.builder()
                .authentication(auth)
                .serviceHttpUrl(serviceHttpUrl)
                .build();
        executor = Executors.newFixedThreadPool(1);
    }

    @AfterAll
    static void afterAll() {
        if (container != null) {
            container.stop();
        }
    }

    @Test
    void testProcessorApp() throws PulsarClientException {
        // create producer
        Producer<Integer> producer = client.newProducer(Schema.INT32)
                .topic(topic)
                .producerName("test-producer")
                .create();

        send(producer, NUMBER_OF_MESSAGES, Function.identity());

        ProcessorApp app = runApplication(config(), ProcessorApp.class);

        // create consumer
        Consumer<Integer> consumer = client.newConsumer(Schema.INT32)
                .topic(topic + "-sink")
                .subscriptionName("subscription")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .consumerName("test-consumer")
                .subscribe();

        // gather consumes messages
        List<Integer> received = new CopyOnWriteArrayList<>();
        receive(consumer, NUMBER_OF_MESSAGES, message -> {
            try {
                received.add(message.getValue());
                consumer.acknowledge(message);
            } catch (Exception e) {
                consumer.negativeAcknowledge(message);
            }
        });

        // wait until we have gathered all the expected messages
        await().until(() -> received.size() >= NUMBER_OF_MESSAGES);
        Assertions.assertThat(received.stream())
                .containsExactlyElementsOf(IntStream.range(1, 101).boxed().collect(Collectors.toList()));

        await().until(() -> app.getAcked().size() >= NUMBER_OF_MESSAGES);
    }

    MapBasedConfig config() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.subscriptionInitialPosition", SubscriptionInitialPosition.Earliest)
                .with("mp.messaging.incoming.data.schema", "INT32")
                .with("mp.messaging.incoming.data.authPluginClassName",
                        "org.apache.pulsar.client.impl.auth.AuthenticationBasic")
                .with("mp.messaging.incoming.data.authParams", "{\"userId\":\"superuser\",\"password\":\"admin\"}")

                .with("mp.messaging.outgoing.sink.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.serviceUrl", serviceUrl)
                .with("mp.messaging.outgoing.sink.topic", topic + "-sink")
                .with("mp.messaging.outgoing.sink.schema", "INT32")
                .with("mp.messaging.outgoing.sink.authPluginClassName",
                        "org.apache.pulsar.client.impl.auth.AuthenticationBasic")
                .with("mp.messaging.outgoing.sink.authParams", "{\"userId\":\"superuser\",\"password\":\"admin\"}");
    }

    @ApplicationScoped
    public static class ProcessorApp {

        List<Message<Integer>> acked = new CopyOnWriteArrayList<>();

        @Incoming("data")
        @Outgoing("sink")
        public Message<Integer> process(Message<Integer> message) {
            return message.withAck(() -> {
                acked.add(message);
                return message.ack();
            }).withPayload(message.getPayload() + 1);
        }

        public List<Message<Integer>> getAcked() {
            return acked;
        }
    }
}
