package io.smallrye.reactive.messaging.camel.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.HashMap;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.junit.Test;

import io.smallrye.reactive.messaging.camel.CamelConnector;
import io.smallrye.reactive.messaging.camel.CamelTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class CamelSourceTest extends CamelTestBase {

    @Test
    public void testWithSourceUsingARegularCamelEndpoint() {
        addClasses(BeanWithCamelSourceUsingRegularEndpoint.class);
        addConfig(getConfigUsingEndpoint());
        initialize();

        BeanWithCamelSourceUsingRegularEndpoint bean = bean(BeanWithCamelSourceUsingRegularEndpoint.class);
        assertThat(bean.list()).isEmpty();
        CamelContext context = camelContext();
        context.createProducerTemplate().asyncSendBody("seda:out", "a");
        context.createProducerTemplate().asyncSendBody("seda:out", "b");
        context.createProducerTemplate().asyncSendBody("seda:out", "c");

        await().until(() -> bean.list().size() == 3);
        assertThat(bean.list()).contains("a", "b", "c");
    }

    private MapBasedConfig getConfigUsingEndpoint() {
        String prefix = "mp.messaging.incoming.data.";
        Map<String, Object> config = new HashMap<>();
        config.putIfAbsent(prefix + "endpoint-uri", "seda:out");
        config.put(prefix + "connector", CamelConnector.CONNECTOR_NAME);
        return new MapBasedConfig(config);
    }

    @Test
    public void testWithSourceUsingARegularCamelRoute() {
        addClasses(BeanWithCamelSourceUsingRegularRoute.class);
        addConfig(getConfigUsingRoute());
        initialize();

        BeanWithCamelSourceUsingRegularRoute bean = bean(BeanWithCamelSourceUsingRegularRoute.class);
        assertThat(bean.list()).isEmpty();
        CamelContext context = camelContext();
        context.createProducerTemplate().asyncSendBody("seda:in", "a");
        context.createProducerTemplate().asyncSendBody("seda:in", "b");
        context.createProducerTemplate().asyncSendBody("seda:in", "c");

        await().until(() -> bean.list().size() == 3);
        assertThat(bean.list()).contains("A", "B", "C");
    }

    private MapBasedConfig getConfigUsingRoute() {
        String prefix = "mp.messaging.incoming.data.";
        Map<String, Object> config = new HashMap<>();
        config.putIfAbsent(prefix + "endpoint-uri", "seda:out");
        config.put(prefix + "connector", CamelConnector.CONNECTOR_NAME);
        return new MapBasedConfig(config);
    }

    @Test
    public void testWithSourceUsingARSCamelEndpoint() {
        addClasses(BeanWithCamelSourceUsingRSEndpoint.class);
        addConfig(getConfigUsingRS());
        initialize();

        BeanWithCamelSourceUsingRSEndpoint bean = bean(BeanWithCamelSourceUsingRSEndpoint.class);
        assertThat(bean.list()).isEmpty();
        CamelContext context = camelContext();
        context.createProducerTemplate().asyncSendBody("seda:in", "a");
        context.createProducerTemplate().asyncSendBody("seda:in", "b");
        context.createProducerTemplate().asyncSendBody("seda:in", "c");

        await().until(() -> bean.list().size() == 3);
        assertThat(bean.list()).contains("A", "B", "C");
        assertThat(bean.props()).containsExactly("value", "value", "value");
    }

    private MapBasedConfig getConfigUsingRS() {
        String prefix = "mp.messaging.incoming.data.";
        Map<String, Object> config = new HashMap<>();
        config.putIfAbsent(prefix + "endpoint-uri", "reactive-streams:out");
        config.put(prefix + "connector", CamelConnector.CONNECTOR_NAME);
        return new MapBasedConfig(config);
    }

}
