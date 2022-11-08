package io.smallrye.reactive.messaging.rabbitmq;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.rabbitmq.client.LongString;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.rabbitmq.RabbitMQMessage;

/**
 * Holds metadata from an incoming RabbitMQ message.
 */
public class IncomingRabbitMQMetadata {

    private final RabbitMQMessage message;
    private final Map<String, Object> headers;

    /**
     * Constructor.
     *
     * @param message the underlying {@link RabbitMQMessage}
     */
    IncomingRabbitMQMetadata(RabbitMQMessage message) {
        this.message = message;

        // Ensure the message headers are cast appropriately
        final Map<String, Object> incomingHeaders = message.properties().getHeaders();
        headers = (incomingHeaders != null) ? incomingHeaders.entrySet().stream()
                .collect(HashMap::new, (m, e) -> m.put(e.getKey(), mapValue(e.getValue())), HashMap::putAll)
                : new HashMap<>();
    }

    /**
     * Recursive function to root out {@link LongString} values in arbitrary depth nested lists.
     *
     * @param v the value to map
     * @return the mapped value, with any embedded {@link LongString} values transformed to their
     *         regular {@link String} equivalents
     */
    private static Object mapValue(final Object v) {
        if (v instanceof LongString) {
            return v.toString();
        } else if (v instanceof List) {
            return ((List<?>) v).stream().map(IncomingRabbitMQMetadata::mapValue).collect(Collectors.toList());
        } else {
            return v;
        }
    }

    /**
     * Retrieves the header value cast to the required type.
     *
     * @param header the name of the header
     * @param type the required type
     * @param <T> the type
     * @return the cast header value, which may be empty if the header is not present
     * @throws IllegalArgumentException if the header value could not be cast as required
     */
    @SuppressWarnings("unused")
    @Nullable
    public <T> Optional<T> getHeader(final String header, final Class<T> type) {
        final Object value = this.headers.get(header);
        if (value == null) {
            return Optional.empty();
        }
        if (!type.isAssignableFrom(value.getClass())) {
            throw new IllegalArgumentException("Incorrect type specified for header '" +
                    header + "'. Expected [" + type + "] but actual type is [" + value.getClass() + "]");
        }
        //noinspection unchecked
        return Optional.of((T) value);
    }

    /**
     * The headers property is a key/value map that allows for arbitrary, user-defined keys and values.
     * Keys are string values that have a maximum length of 255 characters. Values can be any valid AMQP value
     * type; this includes {@link String}, {@link Long}, {@link Boolean} and {@link List}.
     * <p>
     * List values may contain a mixture of other values of any of the above types (including List).
     * </p>
     *
     * @return the message headers as an unmodifiable {@link Map}
     */
    public Map<String, Object> getHeaders() {
        return Collections.unmodifiableMap(headers);
    }

    /**
     * The RFC-2046 MIME type for the message's application-data section (body).
     * As per RFC-2046 this can contain a charset parameter defining the character encoding used: e.g.,
     * 'text/plain; charset="utf-8"'.
     * <p>
     * If the payload is known to be truly opaque binary data, the content-type should be set to application/octet-stream.
     * <p>
     * Stored in the message properties.
     *
     * @return an {@link Optional} containing the content-type, which may be empty if no content-type was received
     */
    public Optional<String> getContentType() {
        return Optional.ofNullable(message.properties().getContentType());
    }

    /**
     * The content-encoding property is used as a modifier to the content-type.
     * When present, its value indicates what additional content encodings have been applied to the application-data,
     * and thus what decoding mechanisms need to be applied in order to obtain the media-type referenced by the
     * content-type header field.
     * <p>
     * Stored in the message properties.
     *
     * @return an {@link Optional} containing the content-encoding, which may be empty if no content-encoding was received
     */
    public Optional<String> getContentEncoding() {
        return Optional.ofNullable(message.properties().getContentEncoding());
    }

    /**
     * The delivery-mode property.
     * <p>
     * Stored in the message properties.
     *
     * @return an {@link Optional} containing the delivery-mode, which may be empty if no delivery-mode was received
     */
    public Optional<Integer> getDeliveryMode() {
        return Optional.ofNullable(message.properties().getDeliveryMode());
    }

    /**
     * This priority field contains the relative message priority. Higher numbers indicate higher priority messages.
     * Messages with higher priorities may be delivered before those with lower priorities.
     * <p>
     * Stored in the message properties.
     *
     * @return an {@link Optional} containing the priority, which may be empty if no priority was received
     */
    public Optional<Integer> getPriority() {
        return Optional.ofNullable(message.properties().getPriority());
    }

    /**
     * This is a client-specific id that can be used to mark or identify messages between clients.
     * <p>
     * Stored in the message properties.
     *
     * @return an {@link Optional} containing the correlation-id, which may be empty if no correlation-id was received
     */
    public Optional<String> getCorrelationId() {
        return Optional.ofNullable(message.properties().getCorrelationId());
    }

    /**
     * The address of the node to send replies to.
     * <p>
     * Stored in the message properties.
     *
     * @return an {@link Optional} containing the reply-to address, which may be empty if no reply-to was received
     */
    public Optional<String> getReplyTo() {
        return Optional.ofNullable(message.properties().getReplyTo());
    }

    /**
     * The expiration property.
     * <p>
     * Stored in the message properties.
     *
     * @return an {@link Optional} containing the expiration, which may be empty if no expiration was received
     */
    public Optional<String> getExpiration() {
        return Optional.ofNullable(message.properties().getExpiration());
    }

    /**
     * The message-id, if set, uniquely identifies a message within the message system.
     * The message producer is usually responsible for setting the message-id in such a way that it is assured to be
     * globally unique. A broker may discard a message as a duplicate if the value of the message-id matches that of a
     * previously received message sent to the same node.
     * <p>
     * Stored in the message properties.
     *
     * @return an {@link Optional} containing the message-id, which may be empty if no message-id was received
     */
    public Optional<String> getMessageId() {
        return Optional.ofNullable(message.properties().getMessageId());
    }

    /**
     * The absolute time when this message was created, expressed as a {@link ZonedDateTime}
     * with respect to the supplied {@link ZoneId}.
     * <p>
     * Stored in the message properties.
     *
     * @param zoneId the {@link ZoneId} representing the time zone in which the timestamp is to be interpreted
     * @return an {@link Optional} containing the date and time, which may be empty if no timestamp was received
     */
    public Optional<ZonedDateTime> getTimestamp(final ZoneId zoneId) {
        final Optional<Date> timestamp = Optional.ofNullable(message.properties().getTimestamp());
        return timestamp.map(t -> ZonedDateTime.ofInstant(t.toInstant(), zoneId));
    }

    /**
     * The type property.
     * <p>
     * Stored in the message properties.
     *
     * @return an {@link Optional} containing the type, which may be empty if no type was received
     */
    public Optional<String> getType() {
        return Optional.ofNullable(message.properties().getType());
    }

    /**
     * The identity of the user responsible for producing the message.
     * The client sets this value, and it may be authenticated by intermediaries.
     * <p>
     * Stored in the message properties.
     *
     * @return an {@link Optional} containing the user-id, which may be empty if no user-id was received
     */
    public Optional<String> getUserId() {
        return Optional.ofNullable(message.properties().getUserId());
    }

    /**
     * The identity of the application responsible for producing the message.
     * The client sets this value, and it may be authenticated by intermediaries.
     * <p>
     * Stored in the message properties.
     *
     * @return an {@link Optional} containing the app-id, which may be empty if no app-id was received
     */
    public Optional<String> getAppId() {
        return Optional.ofNullable(message.properties().getAppId());
    }

    /**
     * The absolute time when this message was created, expressed as a {@link ZonedDateTime}
     * with respect to the supplied {@link ZoneId}.
     * <p>
     * Stored in the message properties.
     *
     * @deprecated Use getTimestamp()
     *
     * @param zoneId the {@link ZoneId} representing the time zone in which the timestamp is to be interpreted
     * @return an {@link Optional} containing the date and time, which may be empty if no timestamp was received
     */
    @Deprecated
    public Optional<ZonedDateTime> getCreationTime(final ZoneId zoneId) {
        final Optional<Date> timestamp = Optional.ofNullable(message.properties().getTimestamp());
        return timestamp.map(t -> ZonedDateTime.ofInstant(t.toInstant(), zoneId));
    }

    /**
     * The message-id, if set, uniquely identifies a message within the message system.
     * The message producer is usually responsible for setting the message-id in such a way that it is assured to be
     * globally unique. A broker may discard a message as a duplicate if the value of the message-id matches that of a
     * previously received message sent to the same node.
     * <p>
     * Stored in the message properties.
     *
     * @deprecated Use getMessageId()
     *
     * @return an {@link Optional} containing the message-id, which may be empty if no message-id was received
     */
    @Deprecated
    public Optional<String> getId() {
        return Optional.ofNullable(message.properties().getMessageId());
    }

    /**
     * The exchange the message was delivered to.
     * <p>
     * Stored in the message envelope.
     *
     * @return the name of the message's exchange.
     */
    public String getExchange() {
        return message.envelope().getExchange();
    }

    /**
     * The routing key for the exchange the message was delivered to.
     * <p>
     * Stored in the message envelope.
     *
     * @return the message's routing key.
     */
    public String getRoutingKey() {
        return message.envelope().getRoutingKey();
    }

    /**
     * This is a hint as to whether this message may have been delivered before (but not acknowledged). If the
     * flag is not set, the message definitely has not been delivered before.
     * If it is set, it may have been delivered before.
     * <p>
     * Stored in the message envelope.
     *
     * @return the message's redelivery flag.
     */
    public boolean isRedeliver() {
        return message.envelope().isRedeliver();
    }
}
