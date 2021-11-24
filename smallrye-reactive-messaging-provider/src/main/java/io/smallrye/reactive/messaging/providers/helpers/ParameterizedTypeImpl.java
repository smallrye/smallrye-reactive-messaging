package io.smallrye.reactive.messaging.providers.helpers;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

/**
 * ParameterizedType implementation class.
 */
final class ParameterizedTypeImpl implements ParameterizedType {
    private final Class<?> raw;
    private final Type useOwner;
    private final List<Type> typeArguments;

    /**
     * Constructor
     *
     * @param raw type
     * @param useOwner owner type to use, if any
     * @param typeArguments formal type arguments
     */
    ParameterizedTypeImpl(Class<?> raw, Type useOwner, List<Type> typeArguments) {
        this.raw = raw;
        this.useOwner = useOwner;
        this.typeArguments = Collections.unmodifiableList(new ArrayList<>(typeArguments));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Type getRawType() {
        return raw;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Type getOwnerType() {
        return useOwner;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Type[] getActualTypeArguments() {
        return typeArguments.toArray(new Type[0]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return TypeUtils.toString(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object obj) {
        return obj == this || obj instanceof ParameterizedType && TypeUtils
                .equals(this, ((ParameterizedType) obj));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int result = 71 << 4;
        result |= raw.hashCode();
        result <<= 4;
        result |= Objects.hashCode(useOwner);
        result <<= 8;
        result |= typeArguments.hashCode();
        return result;
    }
}
