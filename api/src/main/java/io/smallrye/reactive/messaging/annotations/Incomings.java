package io.smallrye.reactive.messaging.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.eclipse.microprofile.reactive.messaging.Incoming;

/**
 * Container annotation to declare several {@link Incoming}.
 * <strong>NOTE:</strong> <em>Experimental</em>, not part of the specification.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Incomings {

    Incoming[] value();

}
