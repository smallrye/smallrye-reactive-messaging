package io.smallrye.reactive.messaging;

/**
 * Interface implemented by metadata class when they can be merged.
 * <p>
 * Note that implementation can simply ignored the merged value and return the first one.
 *
 * @param <T> the type of the metadata class
 */
public interface MergeableMetadata<T> {

    T merge(T other);
}
