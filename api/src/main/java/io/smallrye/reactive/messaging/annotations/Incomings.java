package io.smallrye.reactive.messaging.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.eclipse.microprofile.reactive.messaging.Incoming;

/**
 * This class is used to allow multiple {@link Incoming} declarations.
 * You can either use:
 *
 * <pre>
 * <code>
 * &#64;Incoming("a")
 * &#64;Incoming("b")
 * public void consume(T t);
 * </code>
 * </pre>
 *
 * Or use the {@link Incomings} annotation as container:
 *
 * <pre>
 * <code>
 * &#64;Incomings({
 *    &#64;Incoming("a")
 *    &#64;Incoming("b")
 * })
 * public void consume(T t);
 * </code>
 *
 * </pre>
 *
 * <strong>NOTE:</strong> <em>Experimental</em>, not part of the specification.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Incomings {

    /**
     * @return the array of {@link Incoming}, must not contain {@code null}. All the included {@link Incoming} must be
     *         value (have a non-blank and non-null channel name).
     */
    Incoming[] value();

}
