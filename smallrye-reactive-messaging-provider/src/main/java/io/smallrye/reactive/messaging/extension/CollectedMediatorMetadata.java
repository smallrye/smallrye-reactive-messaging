package io.smallrye.reactive.messaging.extension;

import io.smallrye.reactive.messaging.MediatorConfiguration;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.inject.spi.AnnotatedType;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class CollectedMediatorMetadata {

  private final List<MediatorConfiguration> mediators = new ArrayList<>();

  void add(AnnotatedType type, Method method, Set<Annotation> qualifiers) {
    mediators.add(createMediatorConfiguration(type, method, qualifiers));
  }

  private MediatorConfiguration createMediatorConfiguration(AnnotatedType type, Method met, Set<Annotation> qualifiers) {
    MediatorConfiguration configuration = new MediatorConfiguration(met, type.getJavaClass(), qualifiers);
    configuration.compute(met.getAnnotation(Incoming.class), met.getAnnotation(Outgoing.class));
    return configuration;
  }

  public List<MediatorConfiguration> mediators() {
    return mediators;
  }
}
