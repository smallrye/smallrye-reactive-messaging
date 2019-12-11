package io.smallrye.reactive.messaging.http;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Headers;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.reactive.messaging.http.converters.Serializer;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.reactivex.core.MultiMap;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.ext.web.client.HttpRequest;
import io.vertx.reactivex.ext.web.client.WebClient;

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
                .flatMapCompletionStage(m -> send(m).thenCompose(v -> m.ack()).thenApply(v -> m))
                .ignore();
    }

    @SuppressWarnings("unchecked")
    CompletionStage<Void> send(Message message) {
        Serializer<Object> serializer = Serializer.lookup(message.getPayload(), converterClass);
        HttpRequest request = toHttpRequest(message);
        return serializer.convert(message.getPayload())
                .thenCompose(buffer -> invoke(request, buffer))
                .thenCompose(x -> message.ack());
    }

    @SuppressWarnings("unchecked")
    private HttpRequest toHttpRequest(Message message) {
        Headers headers = message.getHeaders();
        String actualUrl = headers.getAsString(HttpHeaders.URL, this.url);
        String actualMethod = headers.getAsString(HttpHeaders.METHOD, this.method).toUpperCase();
        Map<String, ?> httpHeaders = headers.get(HttpHeaders.HEADERS, Collections.emptyMap());
        Map<String, ?> query = headers.get(HttpHeaders.QUERY_PARAMETERS, Collections.emptyMap());

        HttpRequest request;
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

    private CompletionStage<Void> invoke(HttpRequest<Object> request, Buffer buffer) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        request
                .rxSendBuffer(buffer)
                .subscribe(
                        resp -> {
                            if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                                future.complete(null);
                            } else {
                                LOGGER.debug("HTTP request POST {}  has failed with status code: {}, body is: {}", url,
                                        resp.statusCode(),
                                        resp.body() != null ? resp.body().toString() : "NO CONTENT");
                                future.completeExceptionally(new Exception(
                                        "HTTP request POST " + url + " has not returned a valid status: " + resp.statusCode()));
                            }
                        },
                        future::completeExceptionally);
        return future;
    }

    SubscriberBuilder<? extends Message<?>, Void> sink() {
        return subscriber;
    }
}
