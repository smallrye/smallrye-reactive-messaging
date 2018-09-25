package io.smallrye.reactive.messaging.ack;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class AcknowledgmentTestBase extends WeldTestBaseWithoutTails {

  protected List<String> expected = Collections.emptyList();
  protected List<String> acks = Collections.emptyList();

  @Before
  public void initialize() {
    expected = Arrays.asList("a", "b", "c", "d", "e");
    acks = new ArrayList<>(expected);
  }

  public void assertAcknowledgment(SpiedBeanHelper bean, String id) {
    await().until(() -> bean.acknowledged(id).size() == acks.size());
    await().until(() -> bean.received(id).size() == expected.size());
    assertThat(bean.acknowledged(id)).containsExactlyElementsOf(acks);
    assertThat(bean.received(id)).containsExactlyElementsOf(expected);
  }

  public void assertNoAcknowledgment(SpiedBeanHelper bean, String id) {
    await().until(() -> bean.received(id).size() == expected.size());
    assertThat(bean.acknowledged(id)).isEmpty();
    assertThat(bean.received(id)).containsExactlyElementsOf(expected);
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
