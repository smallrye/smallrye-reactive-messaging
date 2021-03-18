package io.smallrye.reactive.messaging.ce;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;

/**
 * Represents Cloud Event metadata
 * See https://github.com/cloudevents/spec/blob/v1.0/spec.md.
 *
 * @param <T> the data type
 */
public interface CloudEventMetadata<T> {

    /**
     * Identifies the event.
     * Mandatory attribute.
     *
     * @return the id, cannot be {@code null}
     */
    String getId();

    /**
     * Identifies the context in which an event happened.
     * Mandatory attribute.
     *
     * @return the source, cannot be {@code null}
     */
    URI getSource();

    /**
     * The version of the CloudEvents specification which the event uses.
     * Mandatory attribute.
     *
     * @return the specification version
     */
    String getSpecVersion();

    /**
     * This attribute contains a value describing the type of event related to the originating occurrence.
     * Mandatory attribute.
     *
     * @return the type
     */
    String getType();

    /**
     * Content type of data value.
     *
     * @return the content type if any, empty if none.
     */
    Optional<String> getDataContentType();

    /**
     * Identifies the schema that data adheres to.
     *
     * @return the schema URI if any, empty if none.
     */
    Optional<URI> getDataSchema();

    /**
     * This describes the subject of the event in the context of the event producer (identified by source).
     *
     * @return the subject if any, empty if none
     */
    Optional<String> getSubject();

    /**
     * Timestamp of when the occurrence happened.
     *
     * @return the timestamp if any, empty if none
     */
    Optional<ZonedDateTime> getTimeStamp();

    /**
     * A CloudEvent may include any number of additional context attributes with distinct names, known as "extension
     * attributes". This method allows retrieving these attributes.
     *
     * @param name the name of the attribute, must not be {@code null}
     * @return the value of the attribute, empty if not present.
     */
    <A> Optional<A> getExtension(String name);

    /**
     * A CloudEvent may include any number of additional context attributes with distinct names, known as "extension
     * attributes". This method allows retrieving these attributes.
     *
     * @return the map of extension attributes, potentially empty.
     */
    Map<String, Object> getExtensions();

    /**
     * The event payload. It is the owner message payload.
     *
     * @return the payload, can be {@code null}
     */
    T getData();

    String CE_ATTRIBUTE_SPEC_VERSION = "specversion";
    String CE_ATTRIBUTE_ID = "id";
    String CE_ATTRIBUTE_SOURCE = "source";
    String CE_ATTRIBUTE_TYPE = "type";
    String CE_ATTRIBUTE_DATA_CONTENT_TYPE = "datacontenttype";
    String CE_ATTRIBUTE_DATA_SCHEMA = "dataschema";
    String CE_ATTRIBUTE_SUBJECT = "subject";
    String CE_ATTRIBUTE_TIME = "time";
    String CE_VERSION_1_0 = "1.0";

}
