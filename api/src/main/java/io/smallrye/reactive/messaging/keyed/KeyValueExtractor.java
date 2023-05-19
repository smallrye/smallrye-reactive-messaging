package io.smallrye.reactive.messaging.keyed;

import java.lang.reflect.Type;

import jakarta.enterprise.inject.spi.Prioritized;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Experimental;

/**
 * Class responsible for extracting of the {@code key} and {@code value} from {@link Message Messages}.
 * When a {@link KeyedMulti} is injected, the framework looks for the extractor to use.
 * The selection is based on the {@link #canExtract(Message, Type, Type)} method, and {@link Prioritized}.
 * <p>
 * The user can create a bean that expose this interface, with a higher priority than the ones provided by the framework.
 */
@Experimental("SmallRye only feature")
public interface KeyValueExtractor extends Prioritized {

    /**
     * The default priority.
     */
    int DEFAULT_PRIORITY = 100;

    /**
     * Checks if the current extractor can handle the given method to extract a key of type {@code keyType} and a
     * value of type {@code valueType}.
     * Note that this method is only called once, for the first received message.
     * Once a matching extractor is found, it is reused for all the other messages.
     * <p>
     * Also, this method is not used when {@link Keyed} is used.
     * <p>
     * This method should not throw an exception, but return {@code false} if it cannot handle the extraction.
     *
     * @param first the first method, cannot be {@code null}
     * @param keyType the type of the key, cannot be {@code null}
     * @param valueType the type of the value, cannot be {@code null}
     * @return {@code true} if the current extract can handle the extraction, {@code false} otherwise
     */
    boolean canExtract(Message<?> first, Type keyType, Type valueType);

    /**
     * Extracts the key from the given message.
     *
     * @param message the message
     * @param keyType the type of the key
     * @return the object of type {@code keyType}
     */
    Object extractKey(Message<?> message, Type keyType);

    /**
     * Extracts the value from the given message.
     *
     * @param message the message
     * @param valueType the type of the value
     * @return the object of type {@code valueType}
     */
    Object extractValue(Message<?> message, Type valueType);

    @Override
    default int getPriority() {
        return DEFAULT_PRIORITY;
    }
}
