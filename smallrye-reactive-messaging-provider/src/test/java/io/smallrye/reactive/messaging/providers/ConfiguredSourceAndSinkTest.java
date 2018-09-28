package io.smallrye.reactive.messaging.providers;

import io.smallrye.reactive.messaging.WeldTestBase;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ConfiguredSourceAndSinkTest extends WeldTestBase {

  @Test
  public void test() {
    weld.addBeanClass(DummyBean.class);
    // Inject a config provider
    weld.addBeanClass(io.smallrye.config.inject.ConfigProducer.class);

    WeldContainer container = weld.initialize();

    assertThat(registry(container).getPublisher("dummy-source")).isNotEmpty();
    assertThat(registry(container).getPublisher("dummy-sink")).isNotEmpty();

    MyDummyFactories bean = container.getBeanManager().createInstance().select(MyDummyFactories.class).get();
    assertThat(bean.list()).containsExactly("8", "10", "12");
    assertThat(bean.gotCompletion()).isTrue();
  }

}
