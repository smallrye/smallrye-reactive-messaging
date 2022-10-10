package io.smallrye.reactive.messaging.kafka.commit;

/**
 * General purpose custom serializer/deserializer interface for state type encapsulated inside the {@link ProcessingState}
 */
public interface ProcessingStateCodec {

    ProcessingState<?> decode(byte[] bytes);

    byte[] encode(ProcessingState<?> object);

    /**
     * Factory for {@link ProcessingStateCodec}
     */
    interface Factory {
        /**
         * Create codec instance from state type
         *
         * @param stateType the state type
         * @return the codec instance for the given state type
         */
        ProcessingStateCodec create(Class<?> stateType);

    }

}
