package io.smallrye.reactive.messaging.providers.connectors;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class InMemoryConnectorWithMergeTest extends WeldTestBase {

    @BeforeEach
    public void install() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("mp.messaging.outgoing.bar.connector", InMemoryConnector.CONNECTOR);
        conf.put("mp.messaging.outgoing.bar.merge", "true");

        installConfig(new MapBasedConfig(conf));
    }

    @AfterEach
    public void cleanup() {
        releaseConfig();
    }

    @Test
    public void testWithStrings() {
        addBeanClass(FirstProducer.class);
        addBeanClass(SecondProducer.class);
        initialize();
        InMemoryConnector bean = container.getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        assertThat(bean).isNotNull();
        InMemorySink<String> bar = bean.sink("bar");
        assertThat(bar.received()).hasSize(6).extracting(Message::getPayload).contains("a", "b", "c", "d", "e", "f");
    }

    @ApplicationScoped
    public static class FirstProducer {

        @Outgoing("bar")
        public Multi<String> produce() {
            return Multi.createFrom().items("a", "b", "c");
        }

    }

    @ApplicationScoped
    public static class SecondProducer {

        @Outgoing("bar")
        public Multi<String> produce() {
            return Multi.createFrom().items("d", "e", "f");
        }

    }

}
