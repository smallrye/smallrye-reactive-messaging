package io.smallrye.reactive.messaging.ack;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class AcknowlegmentTestBase extends WeldTestBaseWithoutTails {

  public void assertAcknowledgment(SpiedBeanHelper bean, String id) {
    await().until(() -> bean.acknowledged(id).size() == 5);
    await().until(() -> bean.received(id).size() == 5);
    System.out.println("Processed: " + bean.received(id));
    System.out.println("Acked: " + bean.acknowledged(id));
    assertThat(bean.acknowledged(id)).containsExactly("a", "b", "c", "d", "e");
    assertThat(bean.received(id)).containsExactly("a", "b", "c", "d", "e");
  }

  public void assertNoAcknowledgment(SpiedBeanHelper bean, String id) {
    await().until(() -> bean.received(id).size() == 5);
    assertThat(bean.acknowledged(id)).isEmpty();
    assertThat(bean.received(id)).containsExactly("a", "b", "c", "d", "e");
  }

  public void assertPostAcknowledgment(SpiedBeanHelper bean, String id) {
    assertAcknowledgment(bean, id);
    assertThat(bean.acknowledgeTimeStamps(id).get(0)).isGreaterThan(bean.receivedTimeStamps(id).get(0));
  }

  public void assertPreAcknowledgment(SpiedBeanHelper bean, String id) {
    assertAcknowledgment(bean, id);
    assertThat(bean.acknowledgeTimeStamps(id).get(0)).isLessThan(bean.receivedTimeStamps(id).get(0));
  }
}
