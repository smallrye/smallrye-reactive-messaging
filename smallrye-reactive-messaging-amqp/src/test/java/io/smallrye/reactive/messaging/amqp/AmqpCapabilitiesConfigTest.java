package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class AmqpCapabilitiesConfigTest extends AmqpBrokerTestBase {

    private WeldContainer container;
    private final Weld weld = new Weld();

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.shutdown();
        }

        MapBasedConfig.cleanup();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    /**
     * see {@code ProtonServerSenderContext} for sender capabilities
     */
    @Test
    public void testProduceWithCapabilities() {
        String address = UUID.randomUUID().toString();

        List<Integer> messages = new ArrayList<>();
        usage.consumeIntegers(address, messages::add);

        new MapBasedConfig()
                .with("amqp-username", username)
                .with("amqp-password", password)
                .with("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.address", address)
                .with("mp.messaging.outgoing.sink.host", host)
                .with("mp.messaging.outgoing.sink.port", port)
                .with("mp.messaging.outgoing.sink.use-anonymous-sender", false)
                .with("mp.messaging.outgoing.sink.capabilities", "shared,global,some-random-capability")
                .with("mp.messaging.outgoing.sink.tracing-enabled", false)
                .write();

        weld.addBeanClass(ProducingBean.class);
        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));

        await().until(() -> messages.size() >= 10);
    }

    /**
     * see {@code ProtonServerReceiverContext} for receiver capabilities
     */
    @Test
    public void testConsumeWithCapabilities() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("amqp-username", username)
                .with("amqp-password", password)
                .with("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.address", address)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("mp.messaging.incoming.data.capabilities", "topic")
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .write();

        weld.addBeanClass(ConsumptionBean.class);
        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));
        ConsumptionBean bean = container.getBeanManager().createInstance().select(ConsumptionBean.class).get();

        AtomicInteger counter = new AtomicInteger();

        usage.produceTenIntegers(address, counter::incrementAndGet);
        await().until(() -> bean.getResults().size() == 10);

        // Verify routing type via Jolokia REST API
        assertAddressHasMulticastRouting(address);
    }

    private void assertAddressHasMulticastRouting(String address) {
        try {
            String credentials = Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
            String url = String.format(
                    "http://%s:%d/console/jolokia/read/org.apache.activemq.artemis:broker=%%220.0.0.0%%22,component=addresses,address=%%22%s%%22/RoutingTypes",
                    host, managementPort, address);

            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", "Basic " + credentials)
                    .header("Origin", "http://localhost")
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            String body = response.body();
            assertThat(body).contains("MULTICAST");
        } catch (Exception e) {
            throw new RuntimeException("Failed to query Jolokia for address routing type", e);
        }
    }

}
