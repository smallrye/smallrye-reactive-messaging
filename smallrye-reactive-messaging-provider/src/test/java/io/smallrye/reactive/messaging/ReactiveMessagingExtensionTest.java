package io.smallrye.reactive.messaging;

import org.jboss.weld.environment.se.Weld;
import org.junit.Test;

public class ReactiveMessagingExtensionTest {


  @Test
  public void test() {
    Weld weld = new Weld()
      .property("javax.enterprise.inject.scan.implicit", true);
    weld
      .initialize();

    weld.shutdown();
  }

}
