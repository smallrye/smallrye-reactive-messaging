package io.smallrye.reactive.messaging;

import org.junit.Before;

public class WeldTestBase extends WeldTestBaseWithoutTails{

  @Before
  public void setUp() {
    super.setUp();
    weld.addBeanClass(MyCollector.class);
  }

}
