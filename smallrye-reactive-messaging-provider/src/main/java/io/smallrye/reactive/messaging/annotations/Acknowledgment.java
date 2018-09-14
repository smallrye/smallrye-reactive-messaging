package io.smallrye.reactive.messaging.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Temporary annotation - must be copied to spec.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Acknowledgment {

  enum Mode {
    NONE,
    MANUAL,
    PRE_PROCESSING,
    POST_PROCESSING
  }

  Mode value() default Mode.POST_PROCESSING;

}
