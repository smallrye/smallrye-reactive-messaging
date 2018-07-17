package io.smallrye.reactive.messaging;

import org.jboss.weld.environment.se.Weld;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BasicBehaviorTest {


  private static Weld weld;

  @BeforeClass
  public static void startCDI() {
    weld = new Weld()
      .property("javax.enterprise.inject.scan.implicit", true);
    weld
      .initialize();
  }

  @AfterClass
  public static void stopCDI() {
    weld.shutdown();
  }


  @Test
  public void testMethodReceivingAFlowable() {
    assertThat(BeanTestingMethodShapes.methodReceivingAFlowable).containsExactly("2", "3", "4", "5", "6", "7", "8", "9", "10", "11");
  }

  @Test
  public void testMethodReceivingAPublisher() {
    assertThat(BeanTestingMethodShapes.methodReceivingAPublisher).containsExactly("2", "3", "4", "5", "6", "7", "8", "9", "10", "11");
  }

  @Test
  public void testMethodReceivingAPublisherBuilder() {
    assertThat(BeanTestingMethodShapes.methodReceivingAPublisherBuilder).containsExactly("2", "3", "4", "5", "6", "7", "8", "9", "10", "11");
  }

  @Test
  public void testMethodReturningAPublisherBuilder() {
    assertThat(BeanTestingMethodShapes.methodReturningAPublisherBuilder).containsExactly("2", "3", "4", "5", "6", "7", "8", "9", "10", "11");
  }

  @Test
  public void testMethodProducingAProcessorBuilder() {
    assertThat(BeanTestingMethodShapes.methodProducingAProcessorBuilder).containsExactly("2", "3", "4", "5", "6", "7", "8", "9", "10", "11");
  }

  @Test
  public void testMethodProducingAProcessor() {
    assertThat(BeanTestingMethodShapes.methodProducingAProcessor).containsExactly("2", "3", "4", "5", "6", "7", "8", "9", "10", "11");
  }

  @Test
  public void testMethodConsumingItemsAndProducingItems() {
    assertThat(BeanTestingMethodShapes.methodConsumingItemsAndProducingItems).containsExactly("2", "3", "4", "5", "6", "7", "8", "9", "10", "11");
  }

  @Test
  public void testMethodConsumingMessagesAndProducingMessages() {
    assertThat(BeanTestingMethodShapes.methodConsumingMessagesAndProducingMessages).containsExactly("2", "3", "4", "5", "6", "7", "8", "9", "10", "11");
  }

  @Test
  public void testMethodConsumingMessagesAndProducingItems() {
    assertThat(BeanTestingMethodShapes.methodConsumingMessagesAndProducingItems).containsExactly("2", "3", "4", "5", "6", "7", "8", "9", "10", "11");
  }

  @Test
  public void testMethodConsumingItemsAndProducingMessages() {
    assertThat(BeanTestingMethodShapes.methodConsumingItemsAndProducingMessages).containsExactly("2", "3", "4", "5", "6", "7", "8", "9", "10", "11");
  }




}
