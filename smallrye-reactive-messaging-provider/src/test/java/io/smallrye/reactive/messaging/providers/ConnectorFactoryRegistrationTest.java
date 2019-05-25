package io.smallrye.reactive.messaging.providers;

import io.smallrye.reactive.messaging.WeldTestBase;
import io.smallrye.reactive.messaging.spi.ConnectorLiteral;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ConnectorFactoryRegistrationTest extends WeldTestBase {

  @Test
  public void test() {
    initializer.addBeanClasses(DummyBean.class, io.smallrye.config.inject.ConfigProducer.class);

    initialize();

    assertThat(registry(container).getPublishers("dummy-source")).isNotEmpty();
    assertThat(registry(container).getSubscribers("dummy-sink")).isNotEmpty();

    MyDummyFactories bean = container.select(MyDummyFactories.class, ConnectorLiteral.of("dummy")).get();
    assertThat(bean.list()).containsExactly("8", "10", "12");
    assertThat(bean.gotCompletion()).isTrue();
  }

  @Test
  public void testLegacy() {
    initializer.addBeanClasses(DummyBean.class, io.smallrye.config.inject.ConfigProducer.class);

    initialize();

    assertThat(registry(container).getPublishers("legacy-dummy-source")).isNotEmpty();
    assertThat(registry(container).getSubscribers("legacy-dummy-sink")).isNotEmpty();

    MyDummyFactories bean = container.select(MyDummyFactories.class, ConnectorLiteral.of("dummy")).get();
    assertThat(bean.list()).containsExactly("8", "10", "12");
    assertThat(bean.gotCompletion()).isTrue();
  }

}
