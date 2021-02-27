package io.smallrye.reactive.messaging.pulsar;

import java.util.Collections;
import java.util.HashSet;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.junit.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class PulsarTestBase {

    private static final Integer BROKER_PORT = 6650;
    private static final Integer BROKER_HTTP_PORT = 8080;
    private static final String METRICS_ENDPOINT = "/metrics";

    private PulsarClient client;
    private Producer<String> producer;
    private Consumer<String> consumer;

    @ClassRule
    public static GenericContainer pulsarContainer = new GenericContainer("apachepulsar/pulsar")
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
//            Integer brokerPort = pulsarContainer.getMappedPort(BROKER_PORT);
//            Integer restPort = pulsarContainer.getMappedPort(BROKER_HTTP_PORT);
            Integer brokerPort = BROKER_PORT;
            Integer restPort = BROKER_HTTP_PORT;
            final String socketUrl = "pulsar://localhost:" + brokerPort;
            final String restUrl = "http://localhost:" + restPort;

            client = PulsarClient.builder()
                    .serviceUrl(socketUrl)
                    .build();

            PulsarAdmin admin = PulsarAdmin
                    .builder()
                    .serviceHttpUrl(restUrl)
                    .allowTlsInsecureConnection(true)
                    .build();

            admin.tenants().createTenant("smallrye-tenant", getGeneralTenantInfo(admin));
            admin.namespaces().createNamespace("smallrye-tenant/smallrye-namespace/");
            admin.topics().createNonPartitionedTopic("smallrye-tenant/smallrye-namespace/smallrye-topic");

            producer = client.newProducer(Schema.STRING)
                    .topic("test-tenant/test-namespace/test-topic")
                    .create();

            consumer = client.newConsumer(Schema.STRING)
                    .topic("test-tenant/test-namespace/test-topic")
                    .subscriptionName("my-subscription")
                    .subscribe();

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

        } catch (Exception e) {
            Assert.fail("could not create producer");
        }
    }
}
