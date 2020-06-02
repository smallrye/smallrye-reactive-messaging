package io.smallrye.reactive.messaging.annotations;

import static java.lang.annotation.ElementType.METHOD;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Identifies that a particular method performs blocking operations,
 * and as such should be executed on separate worker.
 *
 * When supported, the caller execution context is captured and restored after
 * the execution of the blocking method. This means that the rest of the pipeline
 * would run in the same context (same thread). Typically this is the case if the
 * caller is using a Vert.x Context (event loop or worker).
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
     * Indicates if execution of methods marked with this annotation should be sequenced
     * and preserve the message ordering, or run concurrently with messages being emitted
     * in the order that method executions complete.
     *
     * By default, or when ordered is set to <code>true</code>, executions of a
     * blocking method are serialized and messages are processed
     * in order. The results are emitted in the same order, preserving the relative
     * message order. Multiple different <code>ordered=true</code>
     * methods may be active on workers at one time, but each individual
     * such method will be active once at most.
     *
     * When ordered is set to <code>false</code> blocking method
     * executions are run concurrently on worker threads and may finish in a different
     * order from the order they were invoked. Results are emitted as soon as the blocking
     * computation has finished. Message ordering, whether of input messages being observed
     * by blocking methods running concurrently, or of output messages subsequently emitted,
     * is not preserved.
     *
     * @return whether executions will be ordered.
     */
    boolean ordered() default true;
}
