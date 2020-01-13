package io.smallrye.reactive.messaging.http;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

public class HttpMessage<T> implements Message<T> {

    private final T payload;
    private final Supplier<CompletionStage<Void>> ack;
    private final Metadata metadata;

    public HttpMessage(String method, String url, T payload, Map<String, List<String>> query,
            Map<String, List<String>> headers, Supplier<CompletionStage<Void>> ack) {
        this.payload = payload;
        Metadata.MetadataBuilder builder = Metadata.builder();

        if (method != null) {
            builder.with(HttpMetadata.METHOD, method);
        }
        if (url != null) {
            builder.with(HttpMetadata.URL, url);
        }
        if (headers != null) {
            builder.with(HttpMetadata.HEADERS, headers);
        }
        if (query != null) {
            builder.with(HttpMetadata.QUERY_PARAMETERS, query);
        }

        this.metadata = builder.build();
        this.ack = ack;
    }

    public static <T> HttpMessageBuilder<T> builder() {
        return new HttpMessageBuilder<>();
    }

    @Override
    public T getPayload() {
        return payload;
    }

    public String getMethod() {
        return metadata.getAsString(HttpMetadata.METHOD, null);
    }

    @Override
    public CompletionStage<Void> ack() {
        return ack.get();
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    public Map<String, List<String>> getHeaders() {
        return metadata.get(HttpMetadata.HEADERS, Collections.emptyMap());
    }

    public Map<String, List<String>> getQuery() {
        return metadata.get(HttpMetadata.QUERY_PARAMETERS, Collections.emptyMap());
    }

    public String getUrl() {
        return metadata.getAsString(HttpMetadata.URL, null);
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
