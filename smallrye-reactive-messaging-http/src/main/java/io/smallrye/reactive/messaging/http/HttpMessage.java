package io.smallrye.reactive.messaging.http;


import org.eclipse.microprofile.reactive.messaging.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class HttpMessage<T> implements Message<T> {

  private final T payload;
  private final String method;
  private final Map<String, List<String>> headers;
  private final Map<String, List<String>> query;
  private final String url;

  private HttpMessage(String method, String url, T payload, Map<String, List<String>> query, Map<String, List<String>> headers) {
    this.payload = payload;
    this.method = method;
    this.headers = headers;
    this.query = query;
    this.url = url;
  }

  @Override
  public T getPayload() {
    return payload;
  }

  public String getMethod() {
    return method;
  }

  public Map<String, List<String>> getHeaders() {
    return headers;
  }

  public Map<String, List<String>> getQuery() {
    return query;
  }

  public String getUrl() {
    return url;
  }

  public static final class HttpMessageBuilder<T> {
    private T payload;
    private String method = "POST";
    private Map<String, List<String>> headers;
    private Map<String, List<String>> query;
    private String url;

    private HttpMessageBuilder() {
      headers = new HashMap<>();
      query = new HashMap<>();
    }

    public static <T> HttpMessageBuilder<T> create() {
      return new HttpMessageBuilder<>();
    }

    public HttpMessageBuilder<T> withPayload(T payload) {
      this.payload = payload;
      return this;
    }

    public HttpMessageBuilder<T> withMethod(String method) {
      this.method = Objects.requireNonNull(method);
      return this;
    }

    public HttpMessageBuilder<T> withHeaders(Map<String, List<String>> headers) {
      this.headers = Objects.requireNonNull(headers);
      return this;
    }

    public HttpMessageBuilder<T> withHeader(String key, String value) {
      this.headers.computeIfAbsent(Objects.requireNonNull(key),
        k -> new ArrayList<>())
        .add(Objects.requireNonNull(value));
      return this;
    }

    public HttpMessageBuilder<T> withHeader(String key, List<String> values) {
      this.headers.put(Objects.requireNonNull(key), Objects.requireNonNull(values));
      return this;
    }

    public HttpMessageBuilder<T> withQueryParameter(String key, String value) {
      this.query.computeIfAbsent(Objects.requireNonNull(key),
        k -> new ArrayList<>())
        .add(Objects.requireNonNull(value));
      return this;
    }

    public HttpMessageBuilder<T> withQueryParameter(String key, List<String> values) {
      this.query.put(Objects.requireNonNull(key), Objects.requireNonNull(values));
      return this;
    }


    public HttpMessageBuilder<T> withUrl(String url) {
      this.url = url;
      return this;
    }

    public HttpMessage<T> build() {
      return new HttpMessage<>(method, url, payload, query, headers);
    }
  }
}
