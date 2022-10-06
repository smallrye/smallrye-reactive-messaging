package io.smallrye.reactive.messaging.annotations;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.inject.Qualifier;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.reactive.messaging.MutinyEmitter;

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

    final class Literal extends AnnotationLiteral<EmitterFactoryFor> implements EmitterFactoryFor {

        private final Class<?> value;

        public static EmitterFactoryFor of(Class<?> value) {
            return new Literal(value);
        }

        private Literal(Class<?> value) {
            this.value = value;
        }

        @Override
        public Class<?> value() {
            return value;
        }

        public static EmitterFactoryFor EMITTER = of(org.eclipse.microprofile.reactive.messaging.Emitter.class);
        public static EmitterFactoryFor MUTINY_EMITTER = of(MutinyEmitter.class);
        public static EmitterFactoryFor LEGACY_EMITTER = of(io.smallrye.reactive.messaging.annotations.Emitter.class);
    }

}
