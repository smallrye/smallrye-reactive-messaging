package io.smallrye.reactive.messaging.http;

import static io.smallrye.reactive.messaging.http.i18n.HttpExceptions.ex;
import static io.smallrye.reactive.messaging.http.i18n.HttpLogging.log;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.http.converters.Serializer;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.WebClient;

class HttpSink {

    private final String url;
    private final String method;
    private final WebClient client;
    private final String converterClass;
    private final SubscriberBuilder<? extends Message<?>, Void> subscriber;

    HttpSink(Vertx vertx, HttpConnectorOutgoingConfiguration config) {
        WebClientOptions options = new WebClientOptions(JsonHelper.asJsonObject(config.config()));
        url = config.getUrl();
        if (url == null) {
            throw ex.illegalArgumentUrlNotSet();
        }
        method = config.getMethod();
        client = WebClient.create(vertx, options);
        converterClass = config.getConverter().orElse(null);

        subscriber = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(m -> send(m)
                        .onItem().produceCompletionStage(v -> m.ack())
                        .onItem().apply(x -> m)
                        .subscribeAsCompletionStage())
                .ignore();
    }

    Uni<Void> send(Message<?> message) {
        Serializer<Object> serializer = Serializer.lookup(message.getPayload(), converterClass);
        HttpRequest<?> request = toHttpRequest(message);
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
                throw ex.illegalArgumentInvalidVerb(actualMethod);
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

    private Uni<Void> invoke(HttpRequest<?> request, Buffer buffer) {
        return request
                .sendBuffer(buffer)
                .onItem().apply(resp -> {
                    if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                        return null;
                    } else {
                        log.postFailed(url, resp.statusCode(), resp.body() != null ? resp.body().toString() : "NO CONTENT");
                        throw ex.runtimePostInvalidStatus(url, resp.statusCode());
                    }
                });
    }

    SubscriberBuilder<? extends Message<?>, Void> sink() {
        return subscriber;
    }
}
