package io.smallrye.reactive.messaging.jms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.jboss.weld.environment.util.Collections;
import org.junit.jupiter.api.Test;

public class JmsPropertiesBuilderTest {

    @Test
    public void testCreation() {
        JmsPropertiesBuilder builder = JmsProperties.builder();
        JmsProperties properties = builder
                .with("integer", 2)
                .with("byte", (byte) 3)
                .with("boolean", true)
                .with("short", (short) 4)
                .with("long", 5L)
                .with("float", 6.6f)
                .with("double", 23.4)
                .with("string", "string")
                .with("to-be-removed", "")
                .without("to-be-removed")
                .build();

        assertThat(properties.getIntProperty("integer")).isEqualTo(2);
        assertThat(properties.getByteProperty("byte")).isEqualTo((byte) 3);
        assertThat(properties.getBooleanProperty("boolean")).isTrue();
        assertThat(properties.getShortProperty("short")).isEqualTo((short) 4);
        assertThat(properties.getLongProperty("long")).isEqualTo(5L);
        assertThat(properties.getFloatProperty("float")).isEqualTo(6.6f);
        assertThat(properties.getDoubleProperty("double")).isEqualTo(23.4);
        assertThat(properties.getStringProperty("string")).isEqualTo("string");
        assertThat(properties.getStringProperty("to-be-removed")).isNull();

        assertThat(Collections.asList(properties.getPropertyNames())).hasSize(8);
        assertThat(properties.propertyExists("foo")).isFalse();
        assertThat(properties.propertyExists("string")).isTrue();
    }

    @Test
    public void testCreationWithNullKey() {
        assertThatThrownBy(() -> JmsProperties.builder().with(null, "foo"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testCreationWithNullValue() {
        assertThatThrownBy(() -> JmsProperties.builder().with("hello", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

}
