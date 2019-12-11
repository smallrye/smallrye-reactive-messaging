package io.smallrye.reactive.messaging.http;

/**
 * List the {@link org.eclipse.microprofile.reactive.messaging.Message} headers understood by the HTTP connector.
 * <p>
 * To customize the HTTP request emitted by the connector, you can add these headers to the outgoing message.
 */
@SuppressWarnings("WeakerAccess")
public class HttpHeaders {

    /**
     * The HTTP headers.
     * These headers are added to the produced HTTP request.
     * <p>
     * It must be a {@code Map<String, String|Collection<String>>}.
     * The key of the map is the HTTP header name.
     * The value is either a {@code String} or a {@code Collection<String>} depending the number of values for the
     * header. Single-valued header would use a scalar {@code String}. Multi-valued header would use the collection.
     * The map can mix single-valued and multi-valued headers. Note that the header value must not be {@code null}.
     */
    public static final String HTTP_HEADERS_KEY = "http.headers";

    /**
     * The HTTP Verb / Method.
     * It can be either {@code PUT} or {@code POST}. If not set the method configured on the connector / channel is used.
     * By default, {@code POST} is used.
     */
    public static final String HTTP_METHOD_KEY = "http.method";

    /**
     * The HTTP URL.
     * If not set, it uses the URL configured on the connector / channel.
     */
    public static final String HTTP_URL_KEY = "http.url";

    /**
     * The query parameters to append to the URL.
     *
     * It must be a {@code Map<String, String|Collection<String>>}.
     * The key of the map is the parameter name.
     * The value is either a {@code String} or a {@code Collection<String>} depending the number of values for the
     * parameter. Single-valued parameter would use a scalar {@code String}. Multi-valued parameter would use the
     * collection.
     * The map can mix single-valued and multi-valued parameters. Note that the parameter value must not be {@code null}.
     */
    public static final String HTTP_QUERY_PARAMETERS_KEY = "http.query-parameters";

    private HttpHeaders() {
        // avoid direct instantiation.
    }
}
