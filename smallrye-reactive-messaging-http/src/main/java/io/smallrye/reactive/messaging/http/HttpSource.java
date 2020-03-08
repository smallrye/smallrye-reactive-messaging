package io.smallrye.reactive.messaging.http;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import io.smallrye.mutiny.Multi;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.http.HttpServer;
import io.vertx.mutiny.core.http.HttpServerRequest;

public class HttpSource {

    private final String host;
    private final int port;
    private final Vertx vertx;
    private HttpServer server;

    HttpSource(Vertx vertx, Config config) {
        host = config.getOptionalValue("host", String.class).orElse("0.0.0.0");
        port = config.getOptionalValue("port", Integer.class).orElse(8080);
        this.vertx = vertx;
    }

    PublisherBuilder<? extends Message<?>> source() {
        server = vertx.createHttpServer();

        Multi<HttpServerRequest> multi = Multi.createFrom().emitter(emitter -> server
                .exceptionHandler(emitter::fail)
                .requestHandler(req -> {
                    if (req.path().equalsIgnoreCase("/health")) {
                        req.response().setStatusCode(200).endAndForget(new JsonObject().put("status", "ok").encode());
                    } else {
                        emitter.emit(req);
                    }
                })
                .listen(port, host)
                .onFailure().invoke(emitter::fail)
                .subscribeAsCompletionStage());

        return ReactiveStreams.fromPublisher(multi)
                .flatMapCompletionStage(this::toMessage);
    }

    public void stop() {
        server.closeAndAwait();
    }

    private CompletionStage<HttpMessage<byte[]>> toMessage(HttpServerRequest request) {

        Map<String, List<String>> h = new HashMap<>();
        Map<String, List<String>> q = new HashMap<>();
        MultiMap headers = request.headers();
        MultiMap query = request.params();
        headers.names().forEach(name -> h.put(name, headers.getAll(name)));
        query.names().forEach(name -> q.put(name, query.getAll(name)));

        HttpRequestMetadata meta = new HttpRequestMetadata(
                request.method().name(),
                request.path(),
                h,
                q);

        CompletableFuture<HttpMessage<byte[]>> future = new CompletableFuture<>();
        if (request.method() == HttpMethod.PUT || request.method() == HttpMethod.POST) {
            request.bodyHandler(buffer -> {
                HttpMessage<byte[]> message = new HttpMessage<>(meta, buffer.getBytes(), () -> {
                    // Send the response when the message has been acked.
                    request.response().setStatusCode(202).endAndForget();
                    return CompletableFuture.completedFuture(null);
                });
                future.complete(message);
            });
        } else {
            HttpMessage<byte[]> message = new HttpMessage<>(meta, new byte[0], () -> {
                // Send the response when the message has been acked.
                request.response().setStatusCode(202).endAndForget();
                return CompletableFuture.completedFuture(null);
            });
            future.complete(message);
        }
        return future;
    }

}
