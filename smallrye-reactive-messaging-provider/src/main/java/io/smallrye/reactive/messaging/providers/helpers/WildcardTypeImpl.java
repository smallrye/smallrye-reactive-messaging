package io.smallrye.reactive.messaging.providers.helpers;

import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Arrays;

/**
 * WildcardType implementation class.
 *
 */
final class WildcardTypeImpl implements WildcardType {
    private static final Type[] EMPTY_BOUNDS = new Type[0];

    private final Type[] upperBounds;
    private final Type[] lowerBounds;

    /**
     * Constructor
     *
     * @param upperBounds of this type
     * @param lowerBounds of this type
     */
    WildcardTypeImpl(final Type[] upperBounds, final Type[] lowerBounds) {
        this.upperBounds = upperBounds == null ? EMPTY_BOUNDS : upperBounds;
        this.lowerBounds = lowerBounds == null ? EMPTY_BOUNDS : lowerBounds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Type[] getUpperBounds() {
        return upperBounds.clone();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Type[] getLowerBounds() {
        return lowerBounds.clone();
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
        return obj == this || obj instanceof WildcardType && TypeUtils
                .equals(this, (WildcardType) obj);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int result = 73 << 8;
        result |= Arrays.hashCode(upperBounds);
        result <<= 8;
        result |= Arrays.hashCode(lowerBounds);
        return result;
    }
}
