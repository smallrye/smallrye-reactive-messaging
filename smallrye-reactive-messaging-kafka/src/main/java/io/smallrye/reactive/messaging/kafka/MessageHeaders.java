package io.smallrye.reactive.messaging.kafka;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MessageHeaders {

  private final Headers headers;

  protected MessageHeaders(Headers headers) {
    this.headers = headers;
  }

  public MessageHeaders() {
    this.headers = new RecordHeaders();
  }

  public MessageHeaders put(String key, byte[] value) {
    headers.add(new RecordHeader(key, value));
    return this;
  }

  public MessageHeaders put(String key, ByteBuffer value) {
    headers.add(new RecordHeader(key, value));
    return this;
  }

  public MessageHeaders put(String key, String value) {
    headers.add(new RecordHeader(key, value.getBytes()));
    return this;
  }

  public MessageHeaders put(String key, String value, Charset enc) {
    headers.add(new RecordHeader(key, value.getBytes(enc)));
    return this;
  }

  public MessageHeaders remove(String key) {
    headers.remove(key);
    return this;
  }

  public Optional<byte[]> getOneAsBytes(String key) {
    Header header = headers.lastHeader(Objects.requireNonNull(key, "The `key` must not be `null`"));
    return Optional.ofNullable(header).map(Header::value);
  }

  public Optional<String> getOneAsString(String key) {
    return getOneAsString(key).map(String::new);
  }

  public Optional<String> getOneAsString(String key, Charset enc) {
    return getOneAsBytes(key).map(bytes -> {
      Charset t = Objects.requireNonNull(enc, "The `enc` must not be `null`");
      return new String(bytes, t);
    });
  }

  public List<byte[]> getAllAsBytes(String key) {
    Iterable<Header> list = headers.headers(Objects.requireNonNull(key, "The `key` must not be `null`"));
    return StreamSupport.stream(list.spliterator(), false).map(Header::value).collect(Collectors.toList());
  }

  public List<String> getAllAsStrings(String key) {
    return getAllAsBytes(key).stream().map(String::new).collect(Collectors.toList());
  }

  public List<String> getAllAsStrings(String key, Charset enc) {
    return getAllAsBytes(key).stream().map(bytes -> new String(bytes, Objects.requireNonNull(enc, "`enc` must not be `null`")))
      .collect(Collectors.toList());
  }

  public Iterable<Header> unwrap() {
    return headers;
  }
}
