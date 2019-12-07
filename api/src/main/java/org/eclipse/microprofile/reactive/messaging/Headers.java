package org.eclipse.microprofile.reactive.messaging;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Message header containers.
 * <p>
 * This class stores message metadata that can be related to the transport layer or to the business / application.
 * <p>
 * This class can be seen as an immutable {@link java.util.Map Map<String, Object>}. The content will never change,
 * so there are no remove, put or clear operations. Add new entries using the {@link #with(String, Object)} and remove
 * entries using {@link #without(String)}. These operations returns new instances of {@link Headers}.
 * <p>
 * <p>
 * You can creates new instances using:
 * <ul>
 * <li>{@link #of()} - create instances with 0..5 entries</li>
 * <li>{@link #builder()} - gets a builder in which you can add and remove entries</li>
 * <li>{@link #from(Headers)} - gets a builder containing a copy of the passed headers</li>
 * </ul>
 * <p>
 * Note that:
 * <ul>
 * <li>Keys and values cannot be {@code null}</li>
 * <li>The (insertion-) order of entries is preserved</li>
 * <li>Keys are unique, so changing, via the builder, the value associated with a key, replace it</li>
 * </ul>
 *
 * <strong>IMPORTANT:</strong> Experimental.
 */
public class Headers extends LinkedHashMap<String, Object> {

    /**
     * The empty header singleton.
     */
    private static final Headers EMPTY = new Headers(Collections.emptyMap());

    /**
     * {@link Headers} instances must be created using the static factory methods.
     *
     * @param backend the backend, must not be {@code null}
     */
    private Headers(Map<String, Object> backend) {
        super(backend);
    }

    /**
     * Returns the an empty {@link Headers}
     *
     * @return the new {@link Headers} instance
     */
    public static Headers of() {
        return new HeadersBuilder().build();
    }

    /**
     * Returns a new {@link Headers} containing a single entry.
     *
     * @param k1 the key, must not be {@code null}
     * @param v1 the value, must not be {@code null}
     * @return the new {@link Headers} instance
     */
    public static Headers of(String k1, Object v1) {
        return new HeadersBuilder().with(k1, v1).build();
    }

    /**
     * Returns a new {@link Headers} containing two entries in order.
     *
     * @param k1 the first key, must not be {@code null}
     * @param v1 the first value, must not be {@code null}
     * @param k2 the second key, must not be {@code null}
     * @param v2 the second value, must not be {@code null}
     * @return the new {@link Headers} instance
     * @throws IllegalArgumentException if duplicate keys are provided
     */
    public static Headers of(String k1, Object v1, String k2, Object v2) {
        return new HeadersBuilder().withEntries(entryOf(k1, v1), entryOf(k2, v2)).build();
    }

    /**
     * Returns a new {@link Headers} containing three entries in order.
     *
     * @param k1 the first key, must not be {@code null}
     * @param v1 the first value, must not be {@code null}
     * @param k2 the second key, must not be {@code null}
     * @param v2 the second value, must not be {@code null}
     * @param k3 the third key, must not be {@code null}
     * @param v3 the third value, must not be {@code null}
     * @return the new {@link Headers} instance
     * @throws IllegalArgumentException if duplicate keys are provided
     */
    public static Headers of(String k1, Object v1, String k2, Object v2, String k3, Object v3) {
        return new HeadersBuilder().withEntries(entryOf(k1, v1), entryOf(k2, v2), entryOf(k3, v3)).build();
    }

    /**
     * Returns a new {@link Headers} containing four entries in order.
     *
     * @param k1 the first key, must not be {@code null}
     * @param v1 the first value, must not be {@code null}
     * @param k2 the second key, must not be {@code null}
     * @param v2 the second value, must not be {@code null}
     * @param k3 the third key, must not be {@code null}
     * @param v3 the third value, must not be {@code null}
     * @param k4 the fourth key, must not be {@code null}
     * @param v4 the fourth value, must not be {@code null}
     * @return the new {@link Headers} instance
     * @throws IllegalArgumentException if duplicate keys are provided
     */
    public static Headers of(String k1, Object v1, String k2, Object v2, String k3, Object v3, String k4, Object v4) {
        return new HeadersBuilder().withEntries(
                entryOf(k1, v1), entryOf(k2, v2), entryOf(k3, v3), entryOf(k4, v4)).build();
    }

    /**
     * Returns a new {@link Headers} containing five entries in order.
     *
     * @param k1 the first key, must not be {@code null}
     * @param v1 the first value, must not be {@code null}
     * @param k2 the second key, must not be {@code null}
     * @param v2 the second value, must not be {@code null}
     * @param k3 the third key, must not be {@code null}
     * @param v3 the third value, must not be {@code null}
     * @param k4 the fourth key, must not be {@code null}
     * @param v4 the fourth value, must not be {@code null}
     * @param k5 the fifth key, must not be {@code null}
     * @param v5 the fifth value, must not be {@code null}
     * @return the new {@link Headers} instance
     * @throws IllegalArgumentException if duplicate keys are provided
     */
    public static Headers of(
            String k1, Object v1, String k2, Object v2, String k3, Object v3, String k4, Object v4, String k5, Object v5) {
        return new HeadersBuilder().withEntries(
                entryOf(k1, v1), entryOf(k2, v2), entryOf(k3, v3), entryOf(k4, v4), entryOf(k5, v5)).build();
    }

    /**
     * @return the empty headers instance.
     */
    public static Headers empty() {
        return EMPTY;
    }

    /**
     * Creates a new instance of {@link Headers} with the current entries, plus a new one created from the given key and
     * value. If the given key is already associated with a value, the value is updated in the returned {@link Headers}.
     *
     * @param k the key, must not be {@code null}
     * @param v the value, must not be {@code null}
     * @return the new instance of {@link Headers}
     */
    public Headers with(String k, Object v) {
        return builder().from(this).with(k, v).build();
    }

    /**
     * Creates a new instance of {@link Headers} with the current entries, minus the entry associated with the given key.
     * If the key is not part of the current headers, the same entries are composing the produced instance.
     *
     * @param k the key, must not be {@code null}
     * @return the new instance of {@link Headers}
     */
    public Headers without(String k) {
        return builder().from(this).without(k).build();
    }

    /**
     * Copies the current {@link Headers} instance.
     *
     * @return the new instance.
     */
    public Headers copy() {
        return builder().from(this).build();
    }

    /**
     * @return a builder to create a new instance of {@link Headers}.
     */
    public static HeadersBuilder builder() {
        return new HeadersBuilder();
    }

    /**
     * Creates a new {@link HeadersBuilder} copying the entries of the given instance of {@link Headers}.
     *
     * @param existing the existing instance, must not be {@code null}
     * @return the builder
     */
    public static HeadersBuilder from(Headers existing) {
        return builder().from(existing);
    }

    /**
     * @return the immutable entry set.
     */
    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
        // Return an immutable version of the entry and of the set.
        // so modification to the returned set do not change the headers.
        return super.entrySet().stream().map(entry -> entryOf(entry.getKey(), entry.getValue()))
                .collect(Collectors.toSet());
    }

    /**
     * @return the immutable key set.
     */
    @Override
    public Set<String> keySet() {
        return Collections.unmodifiableSet(super.keySet());
    }

    /**
     * @return the immutable set of values.
     */
    @Override
    public Collection<Object> values() {
        return Collections.unmodifiableCollection(super.values());
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        if (key == null) {
            throw new IllegalArgumentException("The key must not be `null`");
        }
        return get(key) != null;
    }

    public boolean containsKey(String key) {
        if (key == null) {
            throw new IllegalArgumentException("The key must not be `null`");
        }
        return containsKey((Object) key);
    }

    /**
     * Gets the value associated with the given key.
     *
     * @param key the key, must not be {@code null}
     * @return the associated value, {@code null} if not present.
     */
    public Object get(String key) {
        if (key == null) {
            throw new IllegalArgumentException("The key must not be `null`");
        }
        return get((Object) key);
    }

    /**
     * Gets the boolean value associated with the specified key.
     * <p>
     * If the key is not found, {@code false} is returned.
     * If the value is a boolean, the boolean is returned.
     * If the value is a String, the result of {@link Boolean#valueOf(String)} is returned.
     * Otherwise an {@link IllegalArgumentException} is thrown.
     *
     * @param key the key, must not be {@code null}
     * @return the value
     */
    public boolean getAsBoolean(String key) {
        return getAsBoolean(key, false);
    }

    /**
     * Gets the boolean value associated with the specified key.
     * <p>
     * If the key is not found, {@code defaultValue} is returned.
     * If the value is a boolean, the boolean is returned.
     * If the value is a String, the result of {@link Boolean#valueOf(String)} is returned.
     * Otherwise an {@link IllegalArgumentException} is thrown.
     *
     * @param key the key, must not be {@code null}
     * @param defaultValue the default value
     * @return the value
     */
    public boolean getAsBoolean(String key, boolean defaultValue) {
        Object value = get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            return Boolean.valueOf((String) value);
        }
        throw new IllegalArgumentException(
                "The key " + key + " is not associated with a boolean value: " + value.getClass());
    }

    /**
     * Gets the integer value associated with the specified key.
     * <p>
     * If the key is not found, {@code defaultValue} is returned.
     * If the value is a {@link Number}, the result of {@link Number#intValue()} is returned.
     * If the value is a String, the result of {@link Integer#valueOf(String)} is returned, a
     * {@link NumberFormatException} is thrown if the value cannot be read as an integer.
     * Otherwise an {@link IllegalArgumentException} is thrown.
     *
     * @param key the key, must not be {@code null}
     * @param defaultValue the default value
     * @return the value
     */
    public int getAsInteger(String key, int defaultValue) {
        Object value = get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof String) {
            return Integer.valueOf((String) value);
        }
        throw new IllegalArgumentException(
                "The key " + key + " is not associated with an integer value: " + value.getClass());
    }

    /**
     * Gets the long value associated with the specified key.
     * <p>
     * If the key is not found, {@code defaultValue} is returned.
     * If the value is a {@link Number}, the result of {@link Number#longValue()} is returned.
     * If the value is a String, the result of {@link Long#valueOf(String)} is returned, a
     * {@link NumberFormatException} is thrown if the value cannot be read as a long.
     * Otherwise an {@link IllegalArgumentException} is thrown.
     *
     * @param key the key, must not be {@code null}
     * @param defaultValue the default value
     * @return the value
     */
    public long getAsLong(String key, long defaultValue) {
        Object value = get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            return Long.valueOf((String) value);
        }
        throw new IllegalArgumentException(
                "The key " + key + " is not associated with a long value: " + value.getClass());
    }

    /**
     * Gets the double value associated with the specified key.
     * <p>
     * If the key is not found, {@code defaultValue} is returned.
     * If the value is a {@link Number}, the result of {@link Number#doubleValue()} is returned.
     * If the value is a String, the result of {@link Double#valueOf(String)} is returned, a
     * {@link NumberFormatException} is thrown if the value cannot be read as a long.
     * Otherwise an {@link IllegalArgumentException} is thrown.
     *
     * @param key the key, must not be {@code null}
     * @param defaultValue the default value
     * @return the value
     */
    public double getAsDouble(String key, double defaultValue) {
        Object value = get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof String) {
            return Double.valueOf((String) value);
        }
        throw new IllegalArgumentException(
                "The key " + key + " is not associated with a double value: " + value.getClass());
    }

    /**
     * Gets the String value associated with the specified key.
     * <p>
     * If the key is not found, {@code defaultValue} is returned.
     * If the value is a String, the value is returned.
     * Otherwise, {@link Object#toString()} is invoked and the result returned.
     *
     * @param key the key, must not be {@code null}
     * @param defaultValue the default value
     * @return the value
     */
    public String getAsString(String key, String defaultValue) {
        Object value = get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof String) {
            return (String) value;
        }
        return value.toString();
    }

    /**
     * Gets the value associated with the specified key. The value has the expected type.
     * <p>
     * If the key is not found, {@code defaultValue} is returned.
     * If the value is found, the value is casted to the expected type.
     * If the value does not have a compatible type, a {@link ClassCastException} is thrown.
     *
     * @param key the key, must not be {@code null}
     * @param defaultValue the default value
     * @param <T> the expected type
     * @return the value.
     */
    @SuppressWarnings("unchecked")
    public <T> T get(String key, T defaultValue) {
        Object value = get(key);
        if (value == null) {
            return defaultValue;
        }
        return (T) value;
    }

    /**
     * Gets the value associated with the specified key. The value has the expected type.
     * If the key is not found, {@code null} is returned.
     * If the value is found, the value is casted to the expected type.
     * If the value does not have a compatible type, a {@link ClassCastException} is thrown.
     *
     * @return the value, {@code null} if not found
     */
    public <T> T get(String key, Class<T> target) {
        if (target == null) {
            throw new IllegalArgumentException("The target class must not be `null`");
        }
        Object value = get(key);
        if (value == null) {
            return null;
        }
        return target.cast(value);
    }

    /**
     * Guaranteed to throw an exception and leave the map unmodified.
     *
     * @throws UnsupportedOperationException always
     * @deprecated Unsupported operation.
     */
    @Deprecated
    @Override
    public final Object put(String k, Object v) {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the map unmodified.
     *
     * @throws UnsupportedOperationException always
     * @deprecated Unsupported operation.
     */
    @Deprecated
    @Override
    public final Object putIfAbsent(String key, Object value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the map unmodified.
     *
     * @throws UnsupportedOperationException always
     * @deprecated Unsupported operation.
     */
    @Deprecated
    @Override
    public final boolean replace(String key, Object oldValue, Object newValue) {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the map unmodified.
     *
     * @throws UnsupportedOperationException always
     * @deprecated Unsupported operation.
     */
    @Deprecated
    @Override
    public final Object replace(String key, Object value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the map unmodified.
     *
     * @throws UnsupportedOperationException always
     * @deprecated Unsupported operation.
     */
    @Deprecated
    @Override
    public final Object computeIfAbsent(String key, Function<? super String, ? extends Object> mappingFunction) {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the map unmodified.
     *
     * @throws UnsupportedOperationException always
     * @deprecated Unsupported operation.
     */
    @Deprecated
    @Override
    public final Object computeIfPresent(
            String key, BiFunction<? super String, ? super Object, ? extends Object> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the map unmodified.
     *
     * @throws UnsupportedOperationException always
     * @deprecated Unsupported operation.
     */
    @Deprecated
    @Override
    public final Object compute(String key,
            BiFunction<? super String, ? super Object, ? extends Object> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the map unmodified.
     *
     * @throws UnsupportedOperationException always
     * @deprecated Unsupported operation.
     */
    @Deprecated
    @Override
    public final Object merge(
            String key, Object value, BiFunction<? super Object, ? super Object, ? extends Object> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the map unmodified.
     *
     * @throws UnsupportedOperationException always
     * @deprecated Unsupported operation.
     */
    @Deprecated
    @Override
    public final void putAll(Map<? extends String, ? extends Object> map) {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the map unmodified.
     *
     * @throws UnsupportedOperationException always
     * @deprecated Unsupported operation.
     */
    @Deprecated
    @Override
    public final void replaceAll(BiFunction<? super String, ? super Object, ? extends Object> function) {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the map unmodified.
     *
     * @throws UnsupportedOperationException always
     * @deprecated Unsupported operation.
     */
    @Deprecated
    @Override
    public final boolean remove(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the map unmodified.
     *
     * @throws UnsupportedOperationException always
     * @deprecated Unsupported operation.
     */
    @Deprecated
    @Override
    public final Object remove(Object key) {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the map unmodified.
     *
     * @throws UnsupportedOperationException always
     * @deprecated Unsupported operation.
     */
    @Deprecated
    @Override
    public final void clear() {
        throw new UnsupportedOperationException();
    }

    /**
     * Creates an immutable instance of entry.
     *
     * @param k1 the key
     * @param v1 the value
     * @return the immutable entry.
     */
    private static Map.Entry<String, Object> entryOf(String k1, Object v1) {
        return new Map.Entry<String, Object>() {

            @Override
            public String getKey() {
                return k1;
            }

            @Override
            public Object getValue() {
                return v1;
            }

            @Override
            public Object setValue(Object value) {
                throw new UnsupportedOperationException("Updating value is not supported");
            }
        };
    }

    /**
     * A builder to create new instances of {@link Headers}.
     */
    public static class HeadersBuilder {

        private LinkedHashMap<String, Object> backend = new LinkedHashMap<>();

        /**
         * @return a new instance of {@link Headers} with the configured entries.
         */
        public Headers build() {
            return new Headers(backend);
        }

        /**
         * Adds a new entry.
         *
         * @param k the key, must not be {@code null}
         * @param v the value, must not be {@code null}
         * @return this builder
         */
        public HeadersBuilder with(String k, Object v) {
            if (k == null) {
                throw new IllegalArgumentException("the key must not be `null`");
            }
            if (v == null) {
                throw new IllegalArgumentException("the value must not be `null`");
            }
            backend.put(k, v);
            return this;
        }

        @SafeVarargs
        private final HeadersBuilder withEntries(Map.Entry<String, Object>... entries) {
            List<String> keys = new ArrayList<>();
            for (Map.Entry<String, Object> entry : entries) {
                String key = entry.getKey();
                if (keys.contains(key)) {
                    throw new IllegalArgumentException("Duplicate key found: " + key);
                }
                with(key, entry.getValue());
                keys.add(key);
            }
            return this;
        }

        /**
         * Remove the entry associated with the given key.
         *
         * @param k the key, must not be {@code null}
         * @return this builder
         */
        public HeadersBuilder without(String k) {
            if (k == null) {
                throw new IllegalArgumentException("the key must not be `null`");
            }
            backend.remove(k);
            return this;
        }

        /**
         * Copies the entries of the given {@code headers}.
         *
         * @param headers the set of headers to copy, must not be {@code null}
         * @return this builder
         */
        public HeadersBuilder from(Headers headers) {
            if (headers == null) {
                throw new IllegalArgumentException("The headers must not be `null`");
            }
            this.backend.putAll(headers);
            return this;
        }
    }

}
