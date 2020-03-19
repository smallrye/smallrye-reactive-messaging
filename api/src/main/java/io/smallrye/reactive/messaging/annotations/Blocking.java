package io.smallrye.reactive.messaging.annotations;

import static java.lang.annotation.ElementType.METHOD;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Identifies that a particular method performs blocking operations,
 * and as such should be executed on a worker thread.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(METHOD)
public @interface Blocking {
    String NO_VALUE = "<no-value>";

    /**
     * Indicates the name of the worker pool to use for execution.
     * By default all executions will be performed on the default worker pool.
     *
     * @return custom worker pool name for blocking execution.
     */
    String value() default NO_VALUE;

    /**
     * Indicates whether the execution on the worker pool should be ordered.
     * By default all executions are ordered.
     *
     * @return whether executions should be ordered.
     */
    boolean ordered() default true;
}
