package io.smallrye.reactive.messaging.providers;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;

/**
 * The generic parameter types of a method parameter resolve to raw classes, as with the descriptors Quarkus builds at
 * build time: methods consuming streams of connector messages (Multi<IncomingRabbitMQMessage<byte[]>>) have them
 * restored from the element type.
 */
class DefaultMediatorConfigurationErasureTest {

    @SuppressWarnings("unused")
    static void signatures(Multi<Message<byte[]>> parameterized, Multi<? extends Message<String>> wildcard,
            Multi<String> plain, List<?> unbounded) {
    }

    private static Type elementType(int paramIndex) throws NoSuchMethodException {
        Type[] params = DefaultMediatorConfigurationErasureTest.class
                .getDeclaredMethod("signatures", Multi.class, Multi.class, Multi.class, List.class)
                .getGenericParameterTypes();
        return ((ParameterizedType) params[paramIndex]).getActualTypeArguments()[0];
    }

    @Test
    void erasesParameterizedAndWildcardTypeArguments() throws NoSuchMethodException {
        assertThat(DefaultMediatorConfiguration.erasure(elementType(0))).isEqualTo(Message.class);
        assertThat(DefaultMediatorConfiguration.erasure(elementType(1))).isEqualTo(Message.class);
        assertThat(DefaultMediatorConfiguration.erasure(elementType(2))).isEqualTo(String.class);
        assertThat(DefaultMediatorConfiguration.erasure(elementType(3))).isEqualTo(Object.class);
    }
}
