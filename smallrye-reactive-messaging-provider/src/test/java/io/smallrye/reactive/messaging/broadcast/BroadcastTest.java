package io.smallrye.reactive.messaging.broadcast;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class BroadcastTest extends WeldTestBaseWithoutTails  {


  @Test
  public void testBroadcast() {
    addBeanClass(BeanUsingBroadcast.class);
    initialize();

    BeanUsingBroadcast bean = container.getBeanManager().createInstance().select(BeanUsingBroadcast.class).get();

    await().until(() -> bean.l1().size() == 4);
    await().until(() -> bean.l2().size() == 4);

    assertThat(bean.l1()).containsExactly("A", "B", "C", "D").containsExactlyElementsOf(bean.l2());
  }

}
