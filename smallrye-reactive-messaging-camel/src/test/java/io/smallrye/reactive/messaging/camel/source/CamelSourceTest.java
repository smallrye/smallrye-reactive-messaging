package io.smallrye.reactive.messaging.camel.source;

import io.smallrye.reactive.messaging.camel.CamelTestBase;
import org.apache.camel.CamelContext;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class CamelSourceTest extends CamelTestBase {

  @Test
  public void testWithSourceUsingARegularCamelEndpoint() {
    addClasses(BeanWithCamelSourceUsingRegularEndpoint.class);
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

  @Test
  public void testWithSourceUsingARegularCamelRoute() {
    addClasses(BeanWithCamelSourceUsingRegularRoute.class);
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

  @Test
  public void testWithSourceUsingARSCamelEndpoint() {
    addClasses(BeanWithCamelSourceUsingRSEndpoint.class);
    initialize();

    BeanWithCamelSourceUsingRSEndpoint bean = bean(BeanWithCamelSourceUsingRSEndpoint.class);
    assertThat(bean.list()).isEmpty();
    CamelContext context = camelContext();
    context.createProducerTemplate().asyncSendBody("seda:in", "a");
    context.createProducerTemplate().asyncSendBody("seda:in", "b");
    context.createProducerTemplate().asyncSendBody("seda:in", "c");

    await().until(() -> bean.list().size() == 3);
    assertThat(bean.list()).contains("A", "B", "C");
  }


}
