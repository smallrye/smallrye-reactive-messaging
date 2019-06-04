package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.extension.StreamProducer;
import io.smallrye.reactive.messaging.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.impl.LegacyConfiguredChannelFactory;
import io.smallrye.reactive.messaging.connectors.MyDummyConnector;
import org.junit.After;
import org.junit.Before;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import java.util.Collections;
import java.util.List;

public class WeldTestBaseWithoutTails {

  static final List<String> EXPECTED =
    Flowable.range(1, 10).flatMap(i -> Flowable.just(i, i)).map(i -> Integer.toString(i)).toList().blockingGet();

  protected SeContainerInitializer initializer;

  protected SeContainer container;

  @Before
  public void setUp() {
    initializer = SeContainerInitializer.newInstance();

    initializer.addBeanClasses(MediatorFactory.class,
      MediatorManager.class,
      InternalChannelRegistry.class,
      StreamProducer.class,
      ConfiguredChannelFactory.class,
      LegacyConfiguredChannelFactory.class,
      // Messaging provider
      MyDummyConnector.class);

    List<Class> beans = getBeans();
    initializer.addBeanClasses(beans.toArray(new Class<?>[0]));
    initializer.disableDiscovery();
    initializer.addExtensions(new ReactiveMessagingExtension());
  }

  public List<Class> getBeans() {
    return Collections.emptyList();
  }

  @After
  public void tearDown() {
    if (container != null) {
      container.close();
      container = null;
    }
  }

  protected ChannelRegistry registry(SeContainer container) {
    return container.select(ChannelRegistry.class).get();
  }

  public void addBeanClass(Class<?> beanClass) {
    initializer.addBeanClasses(beanClass);
  }

  public void initialize() {
    assert container == null;
    container = initializer.initialize();
  }

  protected <T> T installInitializeAndGet(Class<T> beanClass) {
    initializer.addBeanClasses(beanClass);
    initialize();
    return get(beanClass);
  }

  protected <T> T get(Class<T> c) {
    return container.getBeanManager().createInstance().select(c).get();
  }
}
