package io.smallrye.reactive.messaging.pulsar;

import java.util.Map;
import java.util.Optional;

public interface PulsarMessageMetadata {

    /**
     * Return the properties attached to the message.
     *
     * <p>
     * Properties are application defined key/value pairs that will be attached to the message.
     *
     * @return an unmodifiable view of the properties map
     */
    Map<String, String> getProperties();

    /**
     * Check whether the message has a specific property attached.
     *
     * @param name the name of the property to check
     * @return true if the message has the specified property and false if the properties is not defined
     */
    boolean hasProperty(String name);

    /**
     * Get the value of a specific property.
     *
     * @param name the name of the property
     * @return the value of the property or null if the property was not defined
     */
    String getProperty(String name);

    /**
     * Get the uncompressed message payload size in bytes.
     *
     * @return size in bytes.
     */
    int size();

    /**
     * Get the publish time of this message. The publish time is the timestamp that a client publish the message.
     *
     * @return publish time of this message.
     * @see #getEventTime()
     */
    long getPublishTime();

    /**
     * Get the event time associated with this message.
     *
     * <p>
     * If there isn't any event time associated with this event, it will return 0.
     *
     * @return the message event time or 0 if event time wasn't set
     */
    long getEventTime();

    /**
     * Check whether the message has a key.
     *
     * @return true if the key was set while creating the message and false if the key was not set
     *         while creating the message
     */
    boolean hasKey();

    /**
     * Get the key of the message.
     *
     * @return the key of the message
     */
    String getKey();

    /**
     * Check whether the key has been base64 encoded.
     *
     * @return true if the key is base64 encoded, false otherwise
     */
    boolean hasBase64EncodedKey();

    /**
     * Get bytes in key. If the key has been base64 encoded, it is decoded before being returned.
     * Otherwise, if the key is a plain string, this method returns the UTF_8 encoded bytes of the string.
     *
     * @return the key in byte[] form
     */
    byte[] getKeyBytes();

    /**
     * Check whether the message has a ordering key.
     *
     * @return true if the ordering key was set while creating the message
     *         false if the ordering key was not set while creating the message
     */
    boolean hasOrderingKey();

    /**
     * Get the ordering key of the message.
     *
     * @return the ordering key of the message
     */
    byte[] getOrderingKey();

    /**
     * Get the topic the message was published to.
     *
     * @return the topic the message was published to
     */
    String getTopicName();

    /**
     * Get message redelivery count, redelivery count maintain in pulsar broker. When client acknowledge message
     * timeout, broker will dispatch message again with message redelivery count in CommandMessage defined.
     *
     * <p>
     * Message redelivery increases monotonically in a broker, when topic switch ownership to a another broker
     * redelivery count will be recalculated.
     *
     * @return message redelivery count
     */
    int getRedeliveryCount();

    /**
     * Get schema version of the message.
     *
     * @return Schema version of the message if the message is produced with schema otherwise null.
     */
    byte[] getSchemaVersion();

    /**
     * Check whether the message is replicated from other cluster.
     *
     * @return true if the message is replicated from other cluster.
     *         false otherwise.
     */
    boolean isReplicated();

    /**
     * Get name of cluster, from which the message is replicated.
     *
     * @return the name of cluster, from which the message is replicated.
     */
    String getReplicatedFrom();

    /**
     * Check whether the message has a broker publish time
     *
     * @return true if the message has a broker publish time, otherwise false.
     */
    boolean hasBrokerPublishTime();

    /**
     * Get broker publish time from broker entry metadata.
     * Note that only if the feature is enabled in the broker then the value is available.
     *
     * @return broker publish time from broker entry metadata, or empty if the feature is not enabled in the broker.
     */
    Optional<Long> getBrokerPublishTime();

    /**
     * Check whether the message has an index.
     *
     * @return true if the message has an index, otherwise false.
     */
    boolean hasIndex();

    /**
     * Get index from broker entry metadata.
     * Note that only if the feature is enabled in the broker then the value is available.
     *
     * @return index from broker entry metadata, or empty if the feature is not enabled in the broker.
     */
    Optional<Long> getIndex();
}
