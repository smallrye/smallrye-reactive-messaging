package io.smallrye.reactive.messaging.http;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Metadata for incoming HTTP messages.
 */
public class HttpRequestMetadata {

    /**
     * The HTTP headers.
     */
    private final Map<String, List<String>> headers;

    /**
     * The HTTP Verb / Method.
     */
    private final String method;

    /**
     * The HTTP request path.
     */
    private final String path;

    /**
     * The query parameters to append to the URL.
     */
    private final Map<String, List<String>> query;

    public HttpRequestMetadata(String method, String path,
            Map<String, List<String>> headers,
            Map<String, List<String>> query) {
        this.headers = headers == null ? Collections.emptyMap() : headers;
        this.method = method.toUpperCase();
        this.path = path;
        this.query = query == null ? Collections.emptyMap() : query;
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public String getMethod() {
        return method;
    }

    public String getPath() {
        return path;
    }

    public Map<String, List<String>> getQuery() {
        return query;
    }
}
