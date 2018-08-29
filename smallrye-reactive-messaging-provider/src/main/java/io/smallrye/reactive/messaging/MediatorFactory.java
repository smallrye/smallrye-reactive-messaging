package io.smallrye.reactive.messaging;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class MediatorFactory {

  @Inject
  StreamRegistry registry;


  public Mediator create(MediatorConfiguration configuration) {
    return new Mediator(configuration);
  }

}
