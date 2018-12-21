package io.smallrye.reactive.messaging.extension;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.StreamRegistry;
import io.smallrye.reactive.messaging.annotations.Stream;
import org.apache.commons.lang3.reflect.TypeUtils;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.AnnotatedConstructor;
import javax.enterprise.inject.spi.AnnotatedField;
import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.ProcessAnnotatedType;
import javax.enterprise.util.TypeLiteral;
import javax.inject.Inject;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class PublisherInjectionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PublisherInjectionManager.class);

  private Map<String, CollectedPublisherInjectionMetadata> collected = new HashMap<>();

  private AtomicReference<StreamRegistry> registry = new AtomicReference<>();

  public <T> void analyze(ProcessAnnotatedType<T> pat) {
    LOGGER.debug("Scanning Type: {}", pat.getAnnotatedType().getJavaClass().getName());

    Set<AnnotatedField<? super T>> fields = pat.getAnnotatedType().getFields();
    fields.stream()
      .filter(elem -> elem.isAnnotationPresent(Inject.class)  && elem.isAnnotationPresent(Stream.class))
      .filter(elem -> isValidType(elem.getBaseType()))
      .forEach(elem -> put(elem.getAnnotation(Stream.class).value(), elem.getBaseType()));

    Set<AnnotatedMethod<? super T>> methods = pat.getAnnotatedType().getMethods();
    methods.stream()
      .filter(method -> method.isAnnotationPresent(Inject.class))
      .flatMap(method -> method.getParameters().stream())
      .filter(param -> param.isAnnotationPresent(Stream.class))
      .filter(elem -> isValidType(elem.getBaseType()))
      .forEach(elem -> put(elem.getAnnotation(Stream.class).value(), elem.getBaseType()));

    Set<AnnotatedConstructor<T>> constructors = pat.getAnnotatedType().getConstructors();
    constructors.stream()
      .filter(method -> method.isAnnotationPresent(Inject.class))
      .flatMap(method -> method.getParameters().stream())
      .filter(param -> param.isAnnotationPresent(Stream.class))
      .filter(elem -> isValidType(elem.getBaseType()))
      .forEach(elem -> put(elem.getAnnotation(Stream.class).value(), elem.getBaseType()));
  }

  private void put(String name, Type type) {
    CollectedPublisherInjectionMetadata metadata = collected.computeIfAbsent(name, CollectedPublisherInjectionMetadata::new);
    metadata.setType(type);
  }

  private boolean isValidType(Type type) {
    return TypeUtils.isAssignable(type, Publisher.class)  || TypeUtils.isAssignable(type, PublisherBuilder.class);
  }

  public void setRegistry(StreamRegistry registry) {
    this.registry.set(registry);
  }

  public void createBeans(AfterBeanDiscovery discovery) {
    collected.forEach((name, metadata) -> {
      LOGGER.info("Synthetizing beans for {}", name);
      build(metadata, discovery);
    });
  }

  private Stream named(String name) {
    return new Stream() {

      @Override
      public Class<? extends Annotation> annotationType() {
        return Stream.class;
      }

      @Override
      public String value() {
        return name;
      }
    };
  }

  private void build(CollectedPublisherInjectionMetadata col, AfterBeanDiscovery discovery) {
    LOGGER.debug("Synthetizing beans for {}", col.getName());

    // We are synthetizing 4 beans:
    // Publisher<Message>, Flowable<Message>
    // Publisher, Flowable
    // PublisherBuilder<Message>
    // PublisherBuilder

    if (col.needFlowableOfMessageBean()) {
      discovery.addBean()
        .beanClass(Flowable.class)
        .addQualifier(named(col.getName()))
        .addType(col.getMessageTypeForFlowable())
        .addType(col.getMessageTypeForPublisher())
        .addType(new TypeLiteral<Flowable<Message>>() {})
        .addType(new TypeLiteral<Publisher<Message>>() {})
        .createWith(i -> Flowable.fromPublisher(get(col.getName())));
    }

    if (col.needPublisherBuilderOfMessageBean()) {
      discovery.addBean()
        .beanClass(PublisherBuilder.class)
        .addQualifier(named(col.getName()))
        .addType(col.getMessageTypeForPublisherBuilder())
        .addType(new TypeLiteral<PublisherBuilder<Message>>() {})
        .createWith(i -> ReactiveStreams.fromPublisher(get(col.getName())));
    }

    if (col.needFlowableOfPayloadBean()) {
      discovery.addBean()
        .beanClass(Flowable.class)
        .addQualifier(named(col.getName()))
        .addType(col.getPayloadTypeForFlowable())
        .addType(col.getPayloadTypeForPublisher())
        .createWith(i -> Flowable.fromPublisher(get(col.getName())).map(Message::getPayload));
    }

    if (col.isNeedPublisherBuilderOfPayloadBean()) {
      discovery.addBean()
        .beanClass(PublisherBuilder.class)
        .addQualifier(named(col.getName()))
        .addType(col.getPayloadTypeForPublisherBuilder())
        .createWith(i -> ReactiveStreams.fromPublisher(get(col.getName())).map(Message::getPayload));
    }
  }

  private Publisher<? extends Message> get(String name) {
    if (registry.get() == null) {
      throw new IllegalStateException("No registry injected");
    }
    List<Publisher<? extends Message>> list = registry.get().getPublishers(name);
    if (list.isEmpty()) {
      throw new IllegalStateException("Unable to find a stream with the name " + name + ", available streams are: " + registry.get().getPublisherNames());
    }

    // TODO Manage merge.
    return list.get(0);
  }
}
