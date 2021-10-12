package io.smallrye.reactive.messaging.camel.outgoing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.camel.Exchange;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.camel.CamelTestBase;

public class OutgoingCamelTest extends CamelTestBase {

    private static final String DESTINATION = "seda:camel";

    private static final String HEADER_KEY = "headerKey";

    private static final String HEADER_VALUE = "headerValue";

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

    @Test
    void testWithBeanDeclaringASimpleProcessorThatForwardIncomingHeaders() {
        addClasses(BeanWithSimpleCamelProcessor.class);
        initialize();
        BeanWithSimpleCamelProcessor bean = bean(BeanWithSimpleCamelProcessor.class);

        camelContext().createProducerTemplate().sendBodyAndHeader(DESTINATION, "a", HEADER_KEY, HEADER_VALUE);
        Exchange exchange = camelContext().createConsumerTemplate().receive(DESTINATION, 5000);

        assertEquals(HEADER_VALUE, exchange.getIn().getHeader(HEADER_KEY));
    }
}
