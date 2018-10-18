package io.smallrye.reactive.messaging.invalid;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.junit.After;
import org.junit.Test;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.spi.DefinitionException;
import javax.enterprise.inject.spi.DeploymentException;

import static io.smallrye.reactive.messaging.extension.MediatorManager.STRICT_MODE_PROPERTY;

public class InvalidConfigurationTest extends WeldTestBaseWithoutTails {

  @After
  public void cleanup() {
    System.clearProperty(STRICT_MODE_PROPERTY);
  }

  @Test(expected = DefinitionException.class)
  public void testEmptyOutgoing() {
    addBeanClass(BeanWithEmptyOutgoing.class);
    initialize();
  }

  @Test(expected = DefinitionException.class)
  public void testEmptyIncoming() {
    addBeanClass(BeanWithEmptyIncoming.class);
    initialize();
  }

  @Test
  public void testIncompleteGraphWithoutStrictMode() {
    addBeanClass(IncompleteGraphBean.class);
    initialize();
  }

  @Test(expected = DeploymentException.class)
  public void testIncompleteGraphWithStrictMode() {
    tearDown();
    System.setProperty(STRICT_MODE_PROPERTY, "true");
    setUp();
    addBeanClass(IncompleteGraphBean.class);
    initialize();
  }

  @Test
  public void testEmptyGraphWithStrictMode() {
    tearDown();
    System.setProperty(STRICT_MODE_PROPERTY, "true");
    setUp();
    initialize();
  }

  @ApplicationScoped
  public static class BeanWithEmptyOutgoing {

    @Outgoing
    public PublisherBuilder<String> source() {
      return ReactiveStreams.of("a", "b", "c");
    }
  }

  @ApplicationScoped
  public static class BeanWithEmptyIncoming {

    @Incoming
    public void source(String x) {
      // Do nothing.
    }
  }

  @ApplicationScoped
  public static class IncompleteGraphBean {
    @Incoming("foo")
    public void source(String x) {
      // Do nothing.
    }

    @Outgoing("not-foo")
    public PublisherBuilder<String> source() {
      return ReactiveStreams.of("a", "b", "c");
    }
  }

}
