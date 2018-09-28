package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.impl.ConfiguredStreamFactory;
import io.smallrye.reactive.messaging.impl.StreamFactoryImpl;
import io.smallrye.reactive.messaging.impl.StreamRegistryImpl;
import io.smallrye.reactive.messaging.providers.MyDummyFactories;
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
                               StreamRegistryImpl.class,
                               StreamFactoryImpl.class,
                               ConfiguredStreamFactory.class,
                               // Messaging provider
                               MyDummyFactories.class);

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

  protected StreamRegistry registry(SeContainer container) {
    return container.select(StreamRegistry.class).get();
  }

  public void addBeanClass(Class<?> beanClass) {
    initializer.addBeanClasses(beanClass);
  }

  public void initialize() {
    assert container == null;
    container = initializer.initialize();
  }

  protected  <T> T installInitializeAndGet(Class<T> beanClass) {
    initializer.addBeanClasses(beanClass);
    initialize();
    return container.getBeanManager().createInstance().select(beanClass).get();
  }
}
