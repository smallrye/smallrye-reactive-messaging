package io.smallrye.reactive.messaging.http;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.reactive.messaging.http.converters.Serializer;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.ext.web.client.HttpRequest;
import io.vertx.reactivex.ext.web.client.WebClient;

class HttpSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSink.class);

    private final String url;
    private final WebClient client;
    private final String converterClass;
    private final SubscriberBuilder<? extends Message<?>, Void> subscriber;

    HttpSink(Vertx vertx, Config config) {
        WebClientOptions options = new WebClientOptions(JsonHelper.asJsonObject(config));
        url = config.getOptionalValue("url", String.class)
                .orElseThrow(() -> new IllegalArgumentException("The `url` must be set"));
        client = WebClient.create(vertx, options);
        converterClass = config.getOptionalValue("converter", String.class).orElse(null);

        subscriber = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(m -> send(m).thenCompose(v -> m.ack()).thenApply(v -> m))
                .ignore();
    }

    CompletionStage<Void> send(Message message) {
        if (message instanceof HttpMessage) {
            Serializer<Object> serializer = Serializer.lookup(message.getPayload(), converterClass);
            HttpRequest request = toRequest((HttpMessage) message);
            return serializer.convert(message.getPayload())
                    .thenCompose(buffer -> invoke(request, buffer))
                    .thenCompose(x -> message.ack());
        } else {
            Object payload = message.getPayload();
            Serializer<Object> serializer = Serializer.lookup(payload, converterClass);
            return serializer.convert(payload)
                    .thenCompose(this::invoke)
                    .thenCompose(x -> message.ack());
        }
    }

    private <T> HttpRequest toRequest(HttpMessage<T> message) {
        String u = message.getUrl() == null ? this.url : message.getUrl();

        HttpRequest request;
        if (message.getMethod() == null) {
            request = client.postAbs(u);
        } else {
            switch (message.getMethod().toUpperCase()) {
                case "POST":
                    request = client.postAbs(u);
                    break;
                case "PUT":
                    request = client.putAbs(u);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Invalid method: " + message.getMethod() + ", only PUT and POST are supported");
            }
        }

        message.getMessageHeaders().forEach((k, v) -> v.forEach(item -> request.headers().add(k, item)));
        message.getQuery().forEach((k, v) -> v.forEach(x -> request.addQueryParam(k, x)));

        return request;
    }

    private CompletionStage<Void> invoke(Buffer buffer) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        client.postAbs(url)
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
