package io.smallrye.reactive.messaging.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

/**
 * This class is used to allow multiple {@link Outgoing} declarations.
 * You can either use:
 *
 * <pre>
 * <code>
 * &#64;Outgoing("a")
 * &#64;Outgoing("b")
 * public void consume(T t);
 * </code>
 * </pre>
 *
 * Or use the {@link Outgoings} annotation as container:
 *
 * <pre>
 * <code>
 * &#64;Outgoings({
 *    &#64;Outgoing("a")
 *    &#64;Outgoing("b")
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
public @interface Outgoings {

    /**
     * @return the array of {@link Outgoing}, must not contain {@code null}. All the included {@link Outgoing} must be
     *         value (have a non-blank and non-null channel name).
     */
    Outgoing[] value();

}
