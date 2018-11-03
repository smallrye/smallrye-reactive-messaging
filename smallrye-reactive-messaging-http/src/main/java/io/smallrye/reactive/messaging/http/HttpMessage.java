package io.smallrye.reactive.messaging.http;


import org.eclipse.microprofile.reactive.messaging.Message;

import java.util.*;

public class HttpMessage<T> implements Message<T> {

  private final T payload;
  private final String method;
  private final Map<String, List<String>> headers;
  private final Map<String, String> query;
  private final String url;

  private HttpMessage(String method, String url, T payload, Map<String, String> query, Map<String, List<String>> headers) {
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

  public Map<String, String> getQuery() {
    return query;
  }

  public String getUrl() {
    return url;
  }

  public static final class HttpMessageBuilder<T> {
    private T payload;
    private String method = "POST";
    private Map<String, List<String>> headers;
    private Map<String, String> query;
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

    public HttpMessageBuilder<T> withQuery(Map<String, String> query) {
      this.query = Objects.requireNonNull(query);
      return this;
    }

    public HttpMessageBuilder<T> withQueryParameter(String key, String value) {
      this.query.put(Objects.requireNonNull(key), Objects.requireNonNull(value));
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
