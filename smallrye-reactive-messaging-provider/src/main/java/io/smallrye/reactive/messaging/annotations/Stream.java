package io.smallrye.reactive.messaging.annotations;

import javax.inject.Qualifier;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Qualifier
@Retention(RetentionPolicy.RUNTIME)
public @interface Stream {

  /**
   * Stream name.
   * @return
   */
  String value();
}
