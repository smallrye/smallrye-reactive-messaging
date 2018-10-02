package io.smallrye.reactive.messaging.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Temporary annotation - must be copied to spec.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Merge {

  enum Mode {
    ONE,
    MERGE,
    CONCAT
  }

  Mode value() default Mode.MERGE;

}
