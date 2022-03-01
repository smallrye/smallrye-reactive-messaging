package org.eclipse.microprofile.reactive.messaging;

import java.util.*;

import io.smallrye.common.annotation.Experimental;

/**
 * Message metadata containers.
 * <p>
 * This class stores message metadata that can be related to the transport layer or to the business / application.
 * <p>
 * Instances of this class are <strong>immutable</strong>. Modification operation returned new instances.
 * Contained instances are not constrained, but should be immutable. Only one instance of each class can be stored,
 * as the class is used to retrieve the metadata.
 * <p>
 * You can create new instances using the {@link #of(Object...)} and {@link #from(Iterable) }methods.
 * <p>
 * <strong>IMPORTANT:</strong> Experimental.
 */
@Experimental("metadata propagation is a SmallRye-specific feature")
public class Metadata implements Iterable<Object> {

    private final Map<Class<?>, Object> backend;

    private static final Metadata EMPTY = new Metadata(Collections.emptyMap());

    /**
     * {@link Metadata} instances must be created using the static factory methods.
     *
     * @param backend the backend, must not be {@code null}, must be immutable.
     */
    private Metadata(Map<Class<?>, Object> backend) {
        this.backend = Collections.unmodifiableMap(backend);
    }

    /**
     * Returns an empty set of metadata.
     *
     * @return the empty instance
     */
    public static Metadata empty() {
        return EMPTY;
    }

    /**
     * Returns an instance of {@link Metadata} containing a single value.
     *
     * @param metadata the metadata to be stored, must not be {@code null}. Should be immutable.
     * @return a new {@link Metadata} instance
     */
    static Metadata of(Object metadata) {
        if (metadata == null) {
            throw new IllegalArgumentException("`metadata` must not be `null`");
        }

        return new Metadata(Collections.singletonMap(metadata.getClass(), metadata));
    }

    /**
     * Returns an instance of {@link Metadata} containing multiple values.
     *
     * @param metadata the metadata, must not be {@code null}, must not contain {@code null},
     *        must not contain multiple objects of the same class
     * @return the new metadata
     */
    public static Metadata of(Object... metadata) {
        if (metadata == null) {
            throw new IllegalArgumentException("`metadata` must not be `null`");
        }

        Map<Class<?>, Object> map = createBackingMap(Arrays.asList(metadata));
        return new Metadata(map);
    }

    /**
     * Returns an instance of {@link Metadata} containing multiple values.
     *
     * @param metadata the metadata, must not be {@code null}, must not contain {@code null},
     *        must not contain multiple objects of the same class
     * @return the new metadata
     */
    public static Metadata from(Iterable<Object> metadata) {
        if (metadata == null) {
            throw new IllegalArgumentException("`metadata` must not be `null`");
        }
        if (metadata instanceof Metadata) {
            return (Metadata) metadata;
        }

        Map<Class<?>, Object> map = createBackingMap(metadata);
        if (map.isEmpty()) {
            return Metadata.empty();
        }
        return new Metadata(map);
    }

    private static Map<Class<?>, Object> createBackingMap(Iterable<Object> metadata) {
        Map<Class<?>, Object> map = new HashMap<>();
        for (Object item : metadata) {
            if (item == null) {
                throw new IllegalArgumentException("One of the metadata items is `null`");
            }
            // Ensure that the class is not used.
            if (map.containsKey(item.getClass())) {
                throw new IllegalArgumentException("Duplicate metadata detected: " + item.getClass().getName());
            }
            map.put(item.getClass(), item);
        }
        return map;
    }

    /**
     * Creates a new instance of {@link Metadata} with the current entries, plus {@code item}.
     * If the current set of metadata contains already an instance of the class of {@code item}, the value is replaced
     * in the returned {@link Metadata}.
     *
     * @param item the metadata to be added, must not be {@code null}.
     * @return the new instance of {@link Metadata}
     */
    public Metadata with(Object item) {
        if (item == null) {
            throw new IllegalArgumentException("`item` must not be `null`");
        }
        Map<Class<?>, Object> copy = new HashMap<>(backend);
        copy.put(item.getClass(), item);
        return new Metadata(copy);
    }

    /**
     * Creates a new instance of {@link Metadata} with the current entries, minus the entry associated with the given class.
     * If there is no instance of the class in the current set of metadata, the same entries are composing returned instance
     * of metadata.
     *
     * @param clazz instance from this class are removed from the metadata.
     * @return the new instance of {@link Metadata}
     */
    public Metadata without(Class<?> clazz) {
        if (clazz == null) {
            throw new IllegalArgumentException("`clazz` must not be `null`");
        }
        Map<Class<?>, Object> copy = new HashMap<>(backend);
        copy.remove(clazz);
        return new Metadata(copy);
    }

    /**
     * Copies the current {@link Metadata} instance.
     *
     * @return the new instance.
     */
    public Metadata copy() {
        Map<Class<?>, Object> copy = new HashMap<>(backend);
        return new Metadata(copy);
    }

    /**
     * Retrieves the metadata associated with given class.
     *
     * @param clazz the class of the metadata to retrieve, must not be {@code null}
     * @return an {@link Optional} containing the associated metadata, empty if none.
     */
    public <T> Optional<T> get(Class<T> clazz) {
        if (clazz == null) {
            throw new IllegalArgumentException("`clazz` must not be `null`");
        }
        return Optional.ofNullable(clazz.cast(backend.get(clazz)));
    }

    /**
     * @return an iterator to traverse the set of metadata. This method will never return {@code null}.
     */
    @Override
    public Iterator<Object> iterator() {
        return backend.values().iterator();
    }
}
