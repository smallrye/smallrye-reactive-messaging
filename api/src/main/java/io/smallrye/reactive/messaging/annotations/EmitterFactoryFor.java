package io.smallrye.reactive.messaging.annotations;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.inject.Qualifier;

import io.smallrye.common.annotation.Experimental;

/**
 * Qualifier for {@link io.smallrye.reactive.messaging.EmitterFactory} implementations.
 * <p>
 * The public emitter interface provided by this qualifier will be associated with
 * its implementation provided by the {@link io.smallrye.reactive.messaging.EmitterFactory}.
 */
@Qualifier
@Retention(RetentionPolicy.RUNTIME)
@Target({ METHOD, FIELD, PARAMETER, TYPE })
@Experimental("smallrye-only")
public @interface EmitterFactoryFor {

    /**
     * @return the emitter interface
     */
    Class<?> value();
}
