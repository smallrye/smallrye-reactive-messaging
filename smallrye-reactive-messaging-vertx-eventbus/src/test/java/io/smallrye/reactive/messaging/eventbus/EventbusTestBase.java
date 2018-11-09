package io.smallrye.reactive.messaging.eventbus;

import io.vertx.reactivex.core.Vertx;
import org.junit.After;
import org.junit.Before;

public class EventbusTestBase {

  Vertx vertx;
  protected EventBusUsage usage;

  @Before
  public void setup() {
    vertx = Vertx.vertx();
    usage = new EventBusUsage(vertx.eventBus().getDelegate());
  }

  @After
  public void tearDown() {
    vertx.close();
  }


}
