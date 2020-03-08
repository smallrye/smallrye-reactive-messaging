package io.smallrye.reactive.messaging.http;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.http.converters.Serializer;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.WebClient;

class HttpSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSink.class);

    private final String url;
    private final String method;
    private final WebClient client;
    private final String converterClass;
    private final SubscriberBuilder<? extends Message<?>, Void> subscriber;

    HttpSink(Vertx vertx, Config config) {
        WebClientOptions options = new WebClientOptions(JsonHelper.asJsonObject(config));
        url = config.getOptionalValue("url", String.class)
                .orElseThrow(() -> new IllegalArgumentException("The `url` must be set"));
        method = config.getOptionalValue("method", String.class)
                .orElse("POST");
        client = WebClient.create(vertx, options);
        converterClass = config.getOptionalValue("converter", String.class).orElse(null);

        subscriber = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(m -> send(m)
                        .onItem().produceCompletionStage(v -> m.ack())
                        .onItem().apply(x -> m)
                        .subscribeAsCompletionStage())
                .ignore();
    }

    @SuppressWarnings("unchecked")
    Uni<Void> send(Message<?> message) {
        Serializer<Object> serializer = Serializer.lookup(message.getPayload(), converterClass);
        HttpRequest request = toHttpRequest(message);
        return serializer.convert(message.getPayload())
                .onItem().produceUni(buffer -> invoke(request, buffer))
                .onItem().produceCompletionStage(x -> message.ack());
    }

    @SuppressWarnings("unchecked")
    private HttpRequest<?> toHttpRequest(Message<?> message) {
        HttpResponseMetadata metadata = message.getMetadata(HttpResponseMetadata.class).orElse(null);
        String actualUrl = metadata != null && metadata.getUrl() != null ? metadata.getUrl() : this.url;
        String actualMethod = metadata != null && metadata.getMethod() != null ? metadata.getMethod().toUpperCase()
                : this.method.toUpperCase();
        Map<String, ?> httpHeaders = metadata != null ? metadata.getHeaders() : Collections.emptyMap();
        Map<String, ?> query = metadata != null ? metadata.getQuery() : Collections.emptyMap();

        HttpRequest<?> request;
        switch (actualMethod) {
            case "POST":
                request = client.postAbs(actualUrl);
                break;
            case "PUT":
                request = client.putAbs(actualUrl);
                break;
            default:
                throw new IllegalArgumentException(
                        "Invalid HTTP Verb: " + actualMethod + ", only PUT and POST are supported");
        }

        MultiMap requestHttpHeaders = request.headers();
        httpHeaders.forEach((k, v) -> {
            if (v instanceof Collection) {
                ((Collection<Object>) v).forEach(item -> requestHttpHeaders.add(k, item.toString()));
            } else {
                requestHttpHeaders.add(k, v.toString());
            }
        });
        query.forEach((k, v) -> {
            if (v instanceof Collection) {
                ((Collection<Object>) v).forEach(item -> request.addQueryParam(k, item.toString()));
            } else {
                request.addQueryParam(k, v.toString());
            }
        });

        return request;
    }

    private Uni<Void> invoke(HttpRequest<Object> request, Buffer buffer) {
        return request
                .sendBuffer(buffer)
                .onItem().apply(resp -> {
                    if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                        return null;
                    } else {
                        LOGGER.debug("HTTP request POST {}  has failed with status code: {}, body is: {}", url,
                                resp.statusCode(),
                                resp.body() != null ? resp.body().toString() : "NO CONTENT");
                        throw new RuntimeException(
                                "HTTP request POST " + url + " has not returned a valid status: " + resp.statusCode());
                    }
                });
    }

    SubscriberBuilder<? extends Message<?>, Void> sink() {
        return subscriber;
    }
}
