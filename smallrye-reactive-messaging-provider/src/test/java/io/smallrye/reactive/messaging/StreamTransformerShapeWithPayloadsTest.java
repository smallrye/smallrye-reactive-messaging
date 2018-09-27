package io.smallrye.reactive.messaging;

import io.smallrye.reactive.messaging.beans.BeanConsumingItemAsFlowableAndPublishingItemAsFlowable;
import io.smallrye.reactive.messaging.beans.BeanConsumingItemAsFlowableAndPublishingItemAsPublisher;
import io.smallrye.reactive.messaging.beans.BeanConsumingItemAsPublisherAndPublishingItemAsFlowable;
import io.smallrye.reactive.messaging.beans.BeanConsumingItemAsPublisherBuilderAndPublishingItemAsPublisherBuilder;
import org.jboss.weld.exceptions.DefinitionException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class StreamTransformerShapeWithPayloadsTest extends WeldTestBase {

  @Test
  public void testBeanConsumingItemAsFlowableAndPublishingItemAsFlowable() {
    initializer.addBeanClasses(BeanConsumingItemAsFlowableAndPublishingItemAsFlowable.class);
    try {
      initializer.initialize();
      fail("Validation error expected");
    } catch (DefinitionException e) {
      assertThat(e).hasMessageContaining("@Incoming").hasMessageContaining("@Outgoing").hasMessageContaining("Publisher<Message<I>>");
    }
  }

  @Test
  public void testBeanConsumingItemAsFlowableAndPublishingItemAsPublisher() {
    initializer.addBeanClasses(BeanConsumingItemAsFlowableAndPublishingItemAsPublisher.class);
    try {
      initializer.initialize();
      fail("Validation error expected");
    } catch (DefinitionException e) {
      assertThat(e).hasMessageContaining("@Incoming").hasMessageContaining("@Outgoing").hasMessageContaining("Publisher<Message<I>>");
    }
  }

  @Test
  public void testBeanConsumingItemAsPublisherAndPublishingItemAsFlowable() {
    initializer.addBeanClasses(BeanConsumingItemAsPublisherAndPublishingItemAsFlowable.class);
    try {
      initializer.initialize();
      fail("Validation error expected");
    } catch (DefinitionException e) {
      assertThat(e).hasMessageContaining("@Incoming").hasMessageContaining("@Outgoing").hasMessageContaining("Publisher<Message<I>>");
    }
  }

  @Test
  public void testBeanConsumingItemAsPublisherBuilderAndPublishingItemAsPublisherBuilder() {
    // Consuming or Producing a stream of payload is not supported (as automatic acknowledgement is non-deterministic)
    initializer.addBeanClasses(BeanConsumingItemAsPublisherBuilderAndPublishingItemAsPublisherBuilder.class);
    try {
      initializer.initialize();
      fail("Validation error expected");
    } catch (DefinitionException e) {
      assertThat(e).hasMessageContaining("@Incoming").hasMessageContaining("@Outgoing").hasMessageContaining("Publisher<Message<I>>");
    }
  }

}
