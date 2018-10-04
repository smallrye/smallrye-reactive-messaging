package io.smallrye.reactive.messaging.extension;

import io.smallrye.reactive.messaging.MediatorConfiguration;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.inject.spi.ProcessAnnotatedType;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

class CollectedMediatorMetadata {

  private final List<MediatorConfiguration> mediators = new ArrayList<>();

  void add(ProcessAnnotatedType pat, Method method) {
    mediators.add(createMediatorConfiguration(pat, method));
  }

  private MediatorConfiguration createMediatorConfiguration(ProcessAnnotatedType pat, Method met) {
    MediatorConfiguration configuration = new MediatorConfiguration(met, pat.getAnnotatedType().getJavaClass());
    configuration.compute(met.getAnnotation(Incoming.class), met.getAnnotation(Outgoing.class));
    return configuration;
  }

  public List<MediatorConfiguration> mediators() {
    return mediators;
  }
}
