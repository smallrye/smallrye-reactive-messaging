package io.smallrye.reactive.messaging.ce;

/**
 * Represents the Cloud Event metadata for an outgoing message.
 * The connector dispatching the message, if this connector supports Cloud Event, should write the outgoing message
 * as a Cloud Event using the attribute set in this metadata.
 *
 * See https://github.com/cloudevents/spec/blob/v1.0/spec.md.
 *
 * @param <T> the data type
 */
public interface OutgoingCloudEventMetadata<T> extends CloudEventMetadata<T> {

    /**
     * Gets a builder to create a new {@code OutgoingCloudEventMetadata}.
     *
     * @param <T> the type of data
     * @return the builder
     */
    static <T> OutgoingCloudEventMetadataBuilder<T> builder() {
        return new OutgoingCloudEventMetadataBuilder<>();
    }

    /**
     * Gets a builder to create a new {@code OutgoingCloudEventMetadata}.
     * The values are copied from the given existing {@code OutgoingCloudEventMetadata}
     *
     * @param <T> the type of data
     * @return the builder
     */
    static <T> OutgoingCloudEventMetadataBuilder<T> from(OutgoingCloudEventMetadata<T> existing) {
        return new OutgoingCloudEventMetadataBuilder<>(existing);
    }

}
