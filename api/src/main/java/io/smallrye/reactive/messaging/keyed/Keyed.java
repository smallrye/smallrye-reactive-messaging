package io.smallrye.reactive.messaging.keyed;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Type;

import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Annotation allowing to specify the class of the {@link KeyValueExtractor} to be used.
 *
 * When used, it by-pass the extractor lookup (based on {@link KeyValueExtractor#canExtract(Message, Type, Type)}
 * and {@link jakarta.enterprise.inject.spi.Prioritized}).
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface Keyed {

    /**
     * @return the class name of {@link KeyValueExtractor} to use.
     */
    Class<? extends KeyValueExtractor> value();

}
