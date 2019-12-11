package io.smallrye.reactive.messaging.kafka;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * Message headers attached to the Kafka records.
 */
public class MessageHeaders {

    private static final MessageHeaders EMPTY = new MessageHeaders(Collections.emptyList());

    private final Headers headers;
    private final boolean immutable;

    protected MessageHeaders(Iterable<Header> headers) {
        this.headers = new RecordHeaders(headers);
        this.immutable = true;
    }

    public static MessageHeadersBuilder builder() {
        return new MessageHeadersBuilder();
    }

    /**
     * Creates an empty header set.
     * 
     * @deprecated use {@link #builder()}
     */
    @Deprecated
    public MessageHeaders() {
        this.headers = new RecordHeaders();
        this.immutable = false;
    }

    static MessageHeaders empty() {
        return EMPTY;
    }

    /**
     * Adds a header.
     *
     * @param key the key
     * @param value the key
     * @return the current {@link MessageHeaders}
     * @deprecated use {@link #builder()}
     */
    @Deprecated
    public MessageHeaders put(String key, byte[] value) {
        if (immutable) {
            throw new UnsupportedOperationException("Cannot modify the set of headers");
        }
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        headers.add(new RecordHeader(key, value));
        return this;
    }

    /**
     * Adds a header.
     *
     * @param key the key
     * @param value the key
     * @return the current {@link MessageHeaders}
     * @deprecated use {@link #builder()}
     */
    @Deprecated
    public MessageHeaders put(String key, ByteBuffer value) {
        if (immutable) {
            throw new UnsupportedOperationException("Cannot modify the set of headers");
        }
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        headers.add(new RecordHeader(key, value));
        return this;
    }

    /**
     * Adds a header.
     *
     * @param key the key
     * @param value the key
     * @return the current {@link MessageHeaders}
     * @deprecated use {@link #builder()}
     */
    @Deprecated
    public MessageHeaders put(String key, String value) {
        if (immutable) {
            throw new UnsupportedOperationException("Cannot modify the set of headers");
        }
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        headers.add(new RecordHeader(key, value.getBytes()));
        return this;
    }

    /**
     * Adds a header.
     *
     * @param key the key
     * @param value the key
     * @param enc the encoding
     * @return the current {@link MessageHeaders}
     * @deprecated use {@link #builder()}
     */
    @Deprecated
    public MessageHeaders put(String key, String value, Charset enc) {
        if (immutable) {
            throw new UnsupportedOperationException("Cannot modify the set of headers");
        }
        Objects.requireNonNull(enc);
        return put(key, value.getBytes(enc));
    }

    /**
     * Removes a header.
     *
     * @param key the key, must not be {@code null}
     * @return the current {@link MessageHeaders}
     * @deprecated use {@link #builder()}
     */
    @Deprecated
    public MessageHeaders remove(String key) {
        if (immutable) {
            throw new UnsupportedOperationException("Cannot modify the set of headers");
        }
        headers.remove(Objects.requireNonNull(key));
        return this;
    }

    /**
     * Gets the first key of the header associated to the given key.
     *
     * @param key the key, must not be {@code null}
     * @return an {@link Optional} empty if the header does not exist, or with the first key.
     */
    public Optional<byte[]> getOneAsBytes(String key) {
        Header header = headers.lastHeader(Objects.requireNonNull(key, "The `key` must not be `null`"));
        return Optional.ofNullable(header).map(Header::value);
    }

    /**
     * Gets the first key of the header associated to the given key.
     *
     * @param key the key, must not be {@code null}
     * @return an {@link Optional} empty if the header does not exist, or with the first key.
     */
    public Optional<String> getOneAsString(String key) {
        return getOneAsString(key, StandardCharsets.UTF_8);
    }

    /**
     * Gets the first key of the header associated to the given key.
     *
     * @param key the key, must not be {@code null}
     * @param enc the encoding, must not be {@code null}
     * @return an {@link Optional} empty if the header does not exist, or with the first key.
     */
    public Optional<String> getOneAsString(String key, Charset enc) {
        return getOneAsBytes(key).map(bytes -> {
            Charset t = Objects.requireNonNull(enc, "The `enc` must not be `null`");
            return new String(bytes, t);
        });
    }

    /**
     * Gets all the values of the header associated to the given key.
     *
     * @param key the key, must not be {@code null}
     * @return the list of values, empty is none
     */
    public List<byte[]> getAllAsBytes(String key) {
        Iterable<Header> list = headers.headers(Objects.requireNonNull(key, "The `key` must not be `null`"));
        return StreamSupport.stream(list.spliterator(), false).map(Header::value).collect(Collectors.toList());
    }

    /**
     * Gets all the values of the header associated to the given key.
     *
     * @param key the key, must not be {@code null}
     * @return the list of values, empty is none
     */
    public List<String> getAllAsStrings(String key) {
        return getAllAsStrings(key, StandardCharsets.UTF_8);
    }

    /**
     * Gets all the values of the header associated to the given key.
     *
     * @param key the key, must not be {@code null}
     * @param enc the encoding, must not be {@code null}
     * @return the list of values, empty is none
     */
    public List<String> getAllAsStrings(String key, Charset enc) {
        return getAllAsBytes(key).stream()
                .map(bytes -> new String(bytes, Objects.requireNonNull(enc, "`enc` must not be `null`")))
                .collect(Collectors.toList());
    }

    public Iterable<Header> unwrap() {
        return headers;
    }

    /**
     *
     * @return a copy of the message headers.
     * @deprecated use {@link #builder()}
     */
    @Deprecated
    public MessageHeaders clone() {
        return new MessageHeaders(headers);
    }
}
