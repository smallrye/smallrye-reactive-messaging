package io.smallrye.reactive.messaging.camel.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.camel.CamelContext;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.weld.environment.se.Weld;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.camel.CamelConnector;
import io.smallrye.reactive.messaging.camel.CamelMessage;
import io.smallrye.reactive.messaging.camel.CamelTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class FailureHandlerTest extends CamelTestBase {

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    private MyReceiverBean deploy() {
        Weld weld = new Weld();
        weld.addBeanClass(MyReceiverBean.class);

        container = weld.initialize();
        return container.getBeanManager().createInstance().select(MyReceiverBean.class).get();
    }

    @Test
    public void testFailStrategy() {
        getFailConfig();
        MyReceiverBean bean = deploy();

        CamelContext context = camelContext();
        context.createProducerTemplate().sendBody("seda:fail", "0");
        context.createProducerTemplate().sendBody("seda:fail", "1");
        context.createProducerTemplate().sendBody("seda:fail", "2");
        context.createProducerTemplate().sendBody("seda:fail", "3");
        context.createProducerTemplate().sendBody("seda:fail", "4");
        context.createProducerTemplate().sendBody("seda:fail", "5");
        context.createProducerTemplate().sendBody("seda:fail", "6");
        context.createProducerTemplate().sendBody("seda:fail", "7");
        context.createProducerTemplate().sendBody("seda:fail", "8");
        context.createProducerTemplate().sendBody("seda:fail", "9");

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 4);
        // Other messages should not have been received.
        assertThat(bean.list()).containsExactly("0", "1", "2", "3");
    }

    @Test
    public void testIgnoreStrategy() {
        getIgnoreConfig();
        MyReceiverBean bean = deploy();

        CamelContext context = camelContext();
        context.createProducerTemplate().sendBody("seda:ignore", "0");
        context.createProducerTemplate().sendBody("seda:ignore", "1");
        context.createProducerTemplate().sendBody("seda:ignore", "2");
        context.createProducerTemplate().sendBody("seda:ignore", "3");
        context.createProducerTemplate().sendBody("seda:ignore", "4");
        context.createProducerTemplate().sendBody("seda:ignore", "5");
        context.createProducerTemplate().sendBody("seda:ignore", "6");
        context.createProducerTemplate().sendBody("seda:ignore", "7");
        context.createProducerTemplate().sendBody("seda:ignore", "8");
        context.createProducerTemplate().sendBody("seda:ignore", "9");

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        // All messages should not have been received.
        assertThat(bean.list()).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

    }

    private void getFailConfig() {
        new MapBasedConfig()
                .with("mp.messaging.incoming.camel.endpoint-uri", "seda:fail")
                .with("mp.messaging.incoming.camel.connector", CamelConnector.CONNECTOR_NAME)
                // fail is the default.
                .write();
    }

    private void getIgnoreConfig() {
        new MapBasedConfig()
                .with("mp.messaging.incoming.camel.endpoint-uri", "seda:ignore")
                .with("mp.messaging.incoming.camel.connector", CamelConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.camel.failure-strategy", "ignore")
                .write();
    }

    @ApplicationScoped
    public static class MyReceiverBean {
        private final List<String> received = new CopyOnWriteArrayList<>();

        private static final List<String> SKIPPED = Arrays.asList("3", "6", "9");

        @Incoming("camel")
        public CompletionStage<Void> process(CamelMessage<String> message) {
            received.add(message.getPayload());
            if (SKIPPED.contains(message.getPayload())) {
                return message.nack(new IllegalArgumentException("nack 3 - " + message.getPayload()));
            }
            return message.ack();
        }

        public List<String> list() {
            return received;
        }

    }
}
