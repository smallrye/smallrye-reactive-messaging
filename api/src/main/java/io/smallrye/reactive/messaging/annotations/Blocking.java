package io.smallrye.reactive.messaging.annotations;

import static java.lang.annotation.ElementType.METHOD;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Identifies that a particular method performs blocking operations,
 * and as such should be executed on separate worker.
 *
 * When possible, the initial threads execution context is _restored_
 * once the blocking operation has completed.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(METHOD)
public @interface Blocking {
    String DEFAULT_WORKER_POOL = "<no-value>";

    /**
     * Indicates the name of the worker pool to use for execution.
     * By default all executions will be performed on the default worker pool.
     *
     * The maximum concurrency of a custom worker pool can be set with the following configuration key:
     * <code>smallrye.messaging.worker.{pool-name}.max-concurrency</code>
     *
     * @return custom worker pool name for blocking execution.
     */
    String value() default DEFAULT_WORKER_POOL;

    /**
     * Indicates whether the execution on the worker pool should be ordered.
     * By default all executions are ordered.
     *
     * The blocking processing of incoming messages is executed on workers.
     * However, some computations may be faster than others.
     * When ordered is set to <code>true</code>, the results are emitted in
     * the same order as the input, preserving the ordering.
     * When ordered is set to <code>false</code>, results are emitted as soon as
     * the blocking computation has terminated,
     * regardless of whether the computation of previous messages has completed.
     *
     * @return whether executions should be ordered.
     */
    boolean ordered() default true;
}
