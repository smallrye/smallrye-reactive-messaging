package io.smallrye.reactive.messaging.test.common.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
@Inherited
@Repeatable(SetEnvironmentVariable.SetEnvironmentVariables.class)
@ExtendWith({ EnvironmentVariableExtension.class })
public @interface SetEnvironmentVariable {
    String key();

    String value();

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.METHOD, ElementType.TYPE })
    @Inherited
    @ExtendWith({ EnvironmentVariableExtension.class })
    public @interface SetEnvironmentVariables {
        SetEnvironmentVariable[] value();
    }
}
