package io.smallrye.reactive.messaging.ce;

/**
 * Represents the Cloud Event metadata from an incoming message.
 * <p>
 * If a connector supporting Cloud Events is able to extract a Cloud Event from the incoming message, these metadata
 * are stored in an instance of this interface and added to the metadata of the created message.
 * <p>
 * See https://github.com/cloudevents/spec/blob/v1.0/spec.md.
 *
 * @param <T> the data type
 */
public interface IncomingCloudEventMetadata<T> extends CloudEventMetadata<T> {

}
