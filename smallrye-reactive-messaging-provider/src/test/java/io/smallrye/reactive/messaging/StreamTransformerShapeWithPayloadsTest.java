package io.smallrye.reactive.messaging;

import io.smallrye.reactive.messaging.beans.*;
import org.jboss.weld.environment.se.WeldContainer;
import org.jboss.weld.exceptions.DefinitionException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class StreamTransformerShapeWithPayloadsTest extends WeldTestBase {

  @Test
  public void testBeanConsumingItemAsFlowableAndPublishingItemAsFlowable() {
    weld.addBeanClass(BeanConsumingItemAsFlowableAndPublishingItemAsFlowable.class);
    try {
      weld.initialize();
      fail("Validation error expected");
    } catch (DefinitionException e) {
      assertThat(e).hasMessageContaining("@Incoming").hasMessageContaining("@Outgoing").hasMessageContaining("Publisher<Message<I>>");
    }
  }

  @Test
  public void testBeanConsumingItemAsFlowableAndPublishingItemAsPublisher() {
    weld.addBeanClass(BeanConsumingItemAsFlowableAndPublishingItemAsPublisher.class);
    try {
      weld.initialize();
      fail("Validation error expected");
    } catch (DefinitionException e) {
      assertThat(e).hasMessageContaining("@Incoming").hasMessageContaining("@Outgoing").hasMessageContaining("Publisher<Message<I>>");
    }
  }

  @Test
  public void testBeanConsumingItemAsPublisherAndPublishingItemAsFlowable() {
    weld.addBeanClass(BeanConsumingItemAsPublisherAndPublishingItemAsFlowable.class);
    try {
      weld.initialize();
      fail("Validation error expected");
    } catch (DefinitionException e) {
      assertThat(e).hasMessageContaining("@Incoming").hasMessageContaining("@Outgoing").hasMessageContaining("Publisher<Message<I>>");
    }
  }

  @Test
  public void testBeanConsumingItemAsPublisherBuilderAndPublishingItemAsPublisherBuilder() {
    // Consuming or Producing a stream of payload is not supported (as automatic acknowledgement is non-deterministic)
    weld.addBeanClass(BeanConsumingItemAsPublisherBuilderAndPublishingItemAsPublisherBuilder.class);
    try {
      weld.initialize();
      fail("Validation error expected");
    } catch (DefinitionException e) {
      assertThat(e).hasMessageContaining("@Incoming").hasMessageContaining("@Outgoing").hasMessageContaining("Publisher<Message<I>>");
    }
  }

}
