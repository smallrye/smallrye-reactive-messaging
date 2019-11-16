package io.smallrye.reactive.messaging.annotations;

import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Configure if the annotated publisher should dispatch the messages to several subscribers.
 *
 * Experimental !
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ METHOD, CONSTRUCTOR, FIELD, PARAMETER })
public @interface Broadcast {

    /**
     * Indicates the number of subscribers required before dispatching the items.
     *
     * @return the value, 0 indicates immediate.
     */
    int value() default 0;

}
