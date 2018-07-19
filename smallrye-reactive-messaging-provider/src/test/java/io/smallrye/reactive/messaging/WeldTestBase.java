package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.impl.ConfiguredStreamFactory;
import io.smallrye.reactive.messaging.impl.StreamFactoryImpl;
import io.smallrye.reactive.messaging.impl.StreamRegistryImpl;
import io.smallrye.reactive.messaging.providers.MyDummyFactories;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Before;

import java.util.List;

public class WeldTestBase {

  protected static final List<String> EXPECTED =
    Flowable.range(1, 10).flatMap(i -> Flowable.just(i, i)).map(i -> Integer.toString(i)).toList().blockingGet();

  protected Weld weld;

  @Before
  public void setUp() {
    weld = new Weld();

    weld.addBeanClass(ConfiguredStreamFactory.class);
    weld.addBeanClass(MediatorFactory.class);
    weld.addBeanClass(StreamRegistryImpl.class);
    weld.addBeanClass(StreamFactoryImpl.class);
    weld.addBeanClass(ConfiguredStreamFactory.class);

    // Messaging provider
    weld.addBeanClass(MyDummyFactories.class);

    weld.addExtension(new ReactiveMessagingExtension());

    weld.addBeanClass(MyCollector.class);
  }

  @After
  public void tearDown() {
    weld.shutdown();
  }

  protected StreamRegistry registry(WeldContainer container) {
    return container.getBeanManager().createInstance().select(StreamRegistry.class).get();
  }
}
