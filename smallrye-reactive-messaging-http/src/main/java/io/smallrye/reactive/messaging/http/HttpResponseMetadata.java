package io.smallrye.reactive.messaging.http;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metadata for outgoing HTTP messages handled by the HTTP connector.
 * <p>
 * To customize the HTTP request emitted by the connector, you can add an instance of {@link HttpResponseMetadata}
 * in the message metadata.
 */
public class HttpResponseMetadata {

    /**
     * The HTTP headers.
     * These headers are added to the produced HTTP request.
     * <p>
     * The key of the map is the HTTP header name.
     * The value is either a {@code String} or a {@code Collection<String>} depending the number of values for the
     * header. Single-valued header would use a scalar {@code String}. Multi-valued header would use the collection.
     * The map can mix single-valued and multi-valued headers. Note that the header value must not be {@code null}.
     */
    private final Map<String, List<String>> headers;

    /**
     * The HTTP Verb / Method.
     * It can be either {@code PUT} or {@code POST}. If not set the method configured on the connector / channel is used.
     * By default, {@code POST} is used.
     */
    private final String method;

    /**
     * The HTTP URL.
     * If not set, it uses the URL configured on the connector / channel.
     */
    private final String url;

    /**
     * The query parameters to append to the URL.
     *
     * The key of the map is the parameter name.
     * The value is either a {@code String} or a {@code Collection<String>} depending the number of values for the
     * parameter. Single-valued parameter would use a scalar {@code String}. Multi-valued parameter would use the
     * collection.
     * The map can mix single-valued and multi-valued parameters. Note that the parameter value must not be {@code null}.
     */
    private final Map<String, List<String>> query;

    public HttpResponseMetadata(Map<String, List<String>> headers, String method, String url,
            Map<String, List<String>> query) {
        this.headers = Collections.unmodifiableMap(headers);
        this.method = method;
        this.url = url;
        this.query = Collections.unmodifiableMap(query);
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public String getMethod() {
        return method;
    }

    public String getUrl() {
        return url;
    }

    public Map<String, List<String>> getQuery() {
        return query;
    }

    public static HttpResponseMetadataBuilder builder() {
        return new HttpResponseMetadataBuilder();
    }

    public static final class HttpResponseMetadataBuilder {
        private Map<String, List<String>> headers;
        private String method = "POST";
        private String url;
        private Map<String, List<String>> query;

        public HttpResponseMetadataBuilder from(HttpResponseMetadata existing) {
            this.headers = new HashMap<>(existing.headers);
            this.query = new HashMap<>(existing.query);
            this.method = existing.method;
            this.url = existing.url;
            return this;
        }

        public HttpResponseMetadataBuilder withHeaders(Map<String, List<String>> headers) {
            this.headers = headers;
            return this;
        }

        public HttpResponseMetadataBuilder withHeader(String key, String value) {
            if (this.headers == null) {
                this.headers = new HashMap<>();
            }
            this.headers.put(key, Collections.singletonList(value));
            return this;
        }

        public HttpResponseMetadataBuilder withoutHeader(String key) {
            if (this.headers != null) {
                this.headers.remove(key);
            }
            return this;
        }

        public HttpResponseMetadataBuilder withMethod(String method) {
            this.method = method;
            return this;
        }

        public HttpResponseMetadataBuilder withUrl(String url) {
            this.url = url;
            return this;
        }

        public HttpResponseMetadataBuilder withQueryParameter(Map<String, List<String>> query) {
            this.query = query;
            return this;
        }

        public HttpResponseMetadataBuilder withQueryParameter(String key, String value) {
            if (this.query == null) {
                this.query = new HashMap<>();
            }
            this.query.put(key, Collections.singletonList(value));
            return this;
        }

        public HttpResponseMetadataBuilder withoutQueryParameter(String key) {
            if (this.query != null) {
                this.query.remove(key);
            }
            return this;
        }

        public HttpResponseMetadata build() {
            return new HttpResponseMetadata(headers, method, url, query);
        }
    }
}
