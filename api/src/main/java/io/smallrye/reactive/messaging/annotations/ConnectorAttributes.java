package io.smallrye.reactive.messaging.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This class is used to allow multiple {@link ConnectorAttribute} declarations.
 * <strong>NOTE:</strong> <em>Experimental</em>, not part of the specification.
 * <p>
 * This class should not be used directly. Instead, multiple {@link ConnectorAttribute} should be used on the
 * class annotated with {@link org.eclipse.microprofile.reactive.messaging.spi.Connector}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ConnectorAttributes {

    /**
     * @return the array of {@link ConnectorAttribute}, must not contain {@code null}.
     */
    ConnectorAttribute[] value();

}
