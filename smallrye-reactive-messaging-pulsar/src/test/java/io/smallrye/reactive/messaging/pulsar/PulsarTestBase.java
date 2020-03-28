package io.smallrye.reactive.messaging.pulsar;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.junit.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.Collections;
import java.util.HashSet;

public class PulsarTestBase {

    private static final Integer BROKER_PORT = 6650;
    private static final Integer BROKER_HTTP_PORT = 8080;
    private static final String METRICS_ENDPOINT = "/metrics";

    PulsarClient client;

    @ClassRule
    public static GenericContainer pulsarContainer = new GenericContainer("apachepulsar/pulsar-all:2.5.0")
            .withExposedPorts(BROKER_PORT, BROKER_HTTP_PORT)
            .withCommand("/pulsar/bin/pulsar", "standalone")
            .waitingFor(Wait.forHttp(METRICS_ENDPOINT)
                    .forStatusCode(200)
                    .forPort(BROKER_HTTP_PORT));

    @Test
    public void testContainer() {
        Assert.assertNotNull(pulsarContainer);
    }

    private TenantInfo getGeneralTenantInfo(PulsarAdmin pulsarAdmin) throws PulsarAdminException {
        return new TenantInfo(new HashSet<>(Collections.emptyList()), new HashSet<>(pulsarAdmin.clusters().getClusters()));
    }

    @Before
    public void setup() {
        try {
            Integer brokerPort = pulsarContainer.getMappedPort(BROKER_PORT);
            Integer restPort = pulsarContainer.getMappedPort(BROKER_HTTP_PORT);
            final String socketUrl = "pulsar://localhost:" + brokerPort;
            final String restUrl = "http://localhost:"+restPort;

            client = PulsarClient.builder()
                    .serviceUrl(socketUrl)
                    .build();

            PulsarAdmin admin = PulsarAdmin
                .builder()
                .serviceHttpUrl(restUrl)
                .allowTlsInsecureConnection(true)
                .build();

            admin.tenants().createTenant("test-tenant", getGeneralTenantInfo(admin));
            admin.namespaces().createNamespace("test-tenant/test-namespace/");
            admin.topics().createNonPartitionedTopic("test-tenant/test-namespace/test-topic");



        } catch (PulsarClientException e) {
            Assert.fail("could not create pulsar client");
        } catch (PulsarAdminException e) {
            Assert.fail("could not create pulsar topic");
        }

    }

    @After
    public void tearDown() throws InterruptedException {

    }

    @Test
    public void consumeMessage() {
        try {
            Consumer<String> consumer = (Consumer<String>) client.newConsumer(Schema.STRING)
                    .topic("test-tenant/test-namespace/test-topic")
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionName("transactional-sub")
                    .subscribe();
            Producer<byte[]> producer = (Producer<byte[]>) client.newProducer()
                    .topic("test-tenant/test-namespace/test-topic")
                    .create();
            producer.newMessage()
                .key("my-message-key")
                .value("my-async-message".getBytes())
                .property("my-key", "my-value")
                .property("my-other-key", "my-other-value")
                .send();

            Message<String> message = consumer.receive();
            consumer.acknowledge(message);

            Assert.assertEquals(String.valueOf(message.getData()), "my-async-message");

        } catch (PulsarClientException e) {
            Assert.fail("could not create producer");
        }
    }

}
