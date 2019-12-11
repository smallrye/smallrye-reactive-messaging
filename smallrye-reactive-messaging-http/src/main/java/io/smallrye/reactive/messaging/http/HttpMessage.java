package io.smallrye.reactive.messaging.http;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Headers;
import org.eclipse.microprofile.reactive.messaging.Message;

public class HttpMessage<T> implements Message<T> {

    private final T payload;
    private final Supplier<CompletionStage<Void>> ack;
    private final Headers headers;

    public HttpMessage(String method, String url, T payload, Map<String, List<String>> query,
            Map<String, List<String>> headers, Supplier<CompletionStage<Void>> ack) {
        this.payload = payload;
        Headers.HeadersBuilder builder = Headers.builder();

        if (method != null) {
            builder.with(HttpHeaders.HTTP_METHOD_KEY, method);
        }
        if (url != null) {
            builder.with(HttpHeaders.HTTP_URL_KEY, url);
        }
        if (headers != null) {
            builder.with(HttpHeaders.HTTP_HEADERS_KEY, headers);
        }
        if (query != null) {
            builder.with(HttpHeaders.HTTP_QUERY_PARAMETERS_KEY, query);
        }

        this.headers = builder.build();
        this.ack = ack;
    }

    @Override
    public T getPayload() {
        return payload;
    }

    public String getMethod() {
        return headers.getAsString(HttpHeaders.HTTP_METHOD_KEY, null);
    }

    @Override
    public CompletionStage<Void> ack() {
        return ack.get();
    }

    @Override
    public Headers getHeaders() {
        return headers;
    }

    public Map<String, List<String>> getHttpHeaders() {
        return headers.get(HttpHeaders.HTTP_HEADERS_KEY, Collections.emptyMap());
    }

    public Map<String, List<String>> getQuery() {
        return headers.get(HttpHeaders.HTTP_QUERY_PARAMETERS_KEY, Collections.emptyMap());
    }

    public String getUrl() {
        return headers.getAsString(HttpHeaders.HTTP_URL_KEY, null);
    }

    public static final class HttpMessageBuilder<T> {
        private T payload;
        private String method = "POST";
        private Map<String, List<String>> headers;
        private Map<String, List<String>> query;
        private String url;
        private Supplier<CompletionStage<Void>> ack = () -> CompletableFuture.completedFuture(null);

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

        public HttpMessageBuilder<T> withAck(Supplier<CompletionStage<Void>> ack) {
            this.ack = ack;
            return this;
        }

        public HttpMessage<T> build() {
            return new HttpMessage<>(method, url, payload, query, headers, ack);
        }
    }
}
