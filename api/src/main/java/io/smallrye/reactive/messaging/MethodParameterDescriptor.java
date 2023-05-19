package io.smallrye.reactive.messaging;

import java.util.List;

/**
 * Represents the types of the method parameters and associated generic parameter if any.
 *
 * If will only represent the generic of level 1. For example, if the parameter is {@code Optional<List<String>>},
 * {@link #getTypes()} returns {@code [java.util.Optional]},
 * and {@link #getGenericParameterType(int, int) getGenericParameterType(0, 0)} returns {@code List}
 */
public interface MethodParameterDescriptor {

    List<Class<?>> getTypes();

    Class<?> getGenericParameterType(int paramIndex, int genericIndex);

}
