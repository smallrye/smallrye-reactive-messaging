package io.smallrye.reactive.messaging.inject;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Stream;

@ApplicationScoped
public class BeanInjectedNonExistentStream {

  @Inject
  @Stream("idonotexit")
  private Flowable<Message<String>> field;

}
