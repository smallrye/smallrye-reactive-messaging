package io.smallrye.reactive.messaging.camel.outgoing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.camel.Exchange;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.RetryingTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.reactive.messaging.camel.CamelConnector;
import io.smallrye.reactive.messaging.camel.CamelTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import np.com.madanpokharel.embed.nats.EmbeddedNatsConfig;
import np.com.madanpokharel.embed.nats.EmbeddedNatsServer;
import np.com.madanpokharel.embed.nats.NatsServerConfig;
import np.com.madanpokharel.embed.nats.NatsVersion;
import np.com.madanpokharel.embed.nats.ServerType;

public class OutgoingCamelTest extends CamelTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OutgoingCamelTest.class);
    private static final String DESTINATION = "seda:camel";
    private static final String HEADER_VALUE = "value";
    private static final String HEADER_KEY = "key";

    @Test
    public void testWithABeanDeclaringACamelPublisher() {
        addClasses(BeanWithCamelPublisher.class);
        initialize();
        BeanWithCamelPublisher bean = bean(BeanWithCamelPublisher.class);

        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "a");
        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "b");
        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "c");
        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "d");

        await().until(() -> bean.values().size() == 4);
        assertThat(bean.values()).contains("a", "b", "c", "d");
    }

    @Test
    public void testWithABeanDeclaringATypedCamelPublisher() {
        addClasses(BeanWithTypedCamelPublisher.class);
        initialize();
        BeanWithTypedCamelPublisher bean = bean(BeanWithTypedCamelPublisher.class);

        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "a");
        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "b");
        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "c");
        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "d");

        await().until(() -> bean.values().size() == 4);
        assertThat(bean.values()).contains("a", "b", "c", "d");
    }

    @Test
    public void testWithBeanDeclaringAReactiveStreamRoute() {
        addClasses(BeanWithCamelReactiveStreamRoute.class);
        initialize();
        BeanWithCamelReactiveStreamRoute bean = bean(BeanWithCamelReactiveStreamRoute.class);

        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "a");
        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "b");
        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "c");
        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "d");

        await().until(() -> bean.values().size() == 4);
        assertThat(bean.values()).contains("A", "B", "C", "D");
    }

    @Test
    public void testWithBeanDeclaringATypedReactiveStreamRoute() {
        addClasses(BeanWithTypedCamelReactiveStreamRoute.class);
        initialize();
        BeanWithTypedCamelReactiveStreamRoute bean = bean(BeanWithTypedCamelReactiveStreamRoute.class);

        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "a");
        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "b");
        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "c");
        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "d");

        await().until(() -> bean.values().size() == 4);
        assertThat(bean.values()).contains("A", "B", "C", "D");
    }

    @Test
    public void testWithBeanDeclaringARegularRoute() {
        addClasses(BeanWithCamelRoute.class);
        initialize();
        BeanWithCamelRoute bean = bean(BeanWithCamelRoute.class);

        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "a");
        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "b");
        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "c");
        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "d");

        await().until(() -> bean.values().size() == 4);
        assertThat(bean.values()).contains("a", "b", "c", "d");
    }

    @Test
    public void testWithBeanDeclaringARegularTypedRoute() {
        addClasses(BeanWithTypedCamelRoute.class);
        initialize();
        BeanWithTypedCamelRoute bean = bean(BeanWithTypedCamelRoute.class);

        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "a");
        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "b");
        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "c");
        camelContext().createProducerTemplate().asyncSendBody(DESTINATION, "d");

        await().until(() -> bean.values().size() == 4);
        assertThat(bean.values()).contains("a", "b", "c", "d");
    }

    @RetryingTest(3)
    void testWithBeanDeclaringASimpleProcessorThatForwardIncomingHeaders() throws Exception {
        addClasses(BeanWithSimpleCamelProcessor.class);

        EmbeddedNatsServer natsServer = startNats();
        try {
            addConfig(getNatsConfig());
            initialize();

            final CompletableFuture<Exchange> completableFuture = CompletableFuture
                    .supplyAsync(
                            () -> camelContext().createConsumerTemplate()
                                    .receive("nats:out?servers=127.0.0.1:7656&connectionTimeout=60000", 60000));
            camelContext().createProducerTemplate()
                    .asyncRequestBodyAndHeader("nats:in?servers=127.0.0.1:7656&connectionTimeout=60000", "a", HEADER_KEY,
                            HEADER_VALUE);

            final Exchange exchange = completableFuture.get(60, TimeUnit.SECONDS);

            assertEquals(new String((byte[]) exchange.getMessage().getBody()), "a");
            assertNotNull(exchange.getMessage().getHeaders());
            assertEquals(HEADER_VALUE, exchange.getMessage().getHeader(HEADER_KEY));
        } finally {
            if (natsServer != null) {
                natsServer.stopServer();
            }
        }
    }

    private EmbeddedNatsServer startNats() throws Exception {
        final EmbeddedNatsConfig config = new EmbeddedNatsConfig.Builder()
                .withNatsServerConfig(
                        new NatsServerConfig.Builder()
                                .withServerType(ServerType.NATS)
                                // use version that supports headers
                                .withNatsVersion(new NatsVersion("v2.6.1"))
                                .withPort(7656)
                                .withHost("127.0.0.1")
                                .build())
                .build();
        final EmbeddedNatsServer natsServer = new EmbeddedNatsServer(config);
        natsServer.startServer();
        return natsServer;
    }

    private MapBasedConfig getNatsConfig() {
        final String inPrefix = "mp.messaging.incoming.in.";
        final String outPrefix = "mp.messaging.outgoing.out.";
        final Map<String, Object> config = new HashMap<>();
        config.putIfAbsent(inPrefix + "endpoint-uri", "nats:in?servers=127.0.0.1:7656&connectionTimeout=60000");
        config.putIfAbsent(outPrefix + "endpoint-uri", "nats:out?servers=127.0.0.1:7656&connectionTimeout=60000");
        config.putIfAbsent(inPrefix + "connector", CamelConnector.CONNECTOR_NAME);
        config.putIfAbsent(outPrefix + "connector", CamelConnector.CONNECTOR_NAME);
        return new MapBasedConfig(config);
    }
}
