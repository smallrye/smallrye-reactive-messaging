package io.smallrye.reactive.messaging.annotations;

import static java.lang.annotation.ElementType.TYPE;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation used to declare an attribute on the connector.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(TYPE)
@Repeatable(ConnectorAttributes.class)
public @interface ConnectorAttribute {

    enum Direction {
        INCOMING,
        OUTGOING,
        INCOMING_AND_OUTGOING
    }

    /**
     * The constant used to indicate that the attribute has no default value or no alias.
     */
    String NO_VALUE = "<no-value>";

    /**
     * @return the attribute name, must not be {@code null}, must not be {@code blank}, must be unique for a specific
     *         connector
     */
    String name();

    /**
     * @return the description of the attribute.
     */
    String description();

    /**
     * @return whether the attribute must be hidden from the documentation.
     */
    boolean hiddenFromDocumentation() default false;

    /**
     * @return whether the attribute is mandatory.
     */
    boolean mandatory() default false;

    /**
     * @return on which direction the attribute is used.
     */
    Direction direction();

    /**
     * @return the default value if any.
     */
    String defaultValue() default NO_VALUE;

    /**
     * @return whether the attribute is deprecated.
     */
    boolean deprecated() default false;

    /**
     * @return the optional MicroProfile Config property used to configure the attribute.
     */
    String alias() default NO_VALUE;

    /**
     * @return the java type of the property.
     */
    String type();
}
