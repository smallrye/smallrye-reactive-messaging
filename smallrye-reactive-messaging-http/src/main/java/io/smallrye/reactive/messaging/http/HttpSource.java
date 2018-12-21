package io.smallrye.reactive.messaging.http;

import io.reactivex.processors.BehaviorProcessor;
import io.smallrye.reactive.messaging.spi.ConfigurationHelper;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.MultiMap;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.http.HttpServer;
import io.vertx.reactivex.core.http.HttpServerRequest;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class HttpSource {

  private final String host;
  private final int port;
  private final Vertx vertx;
  private HttpServer server;

  public HttpSource(Vertx vertx, ConfigurationHelper config) {
    host = config.get("host", "0.0.0.0");
    port = config.getAsInteger("port", 8080);
    this.vertx = vertx;
  }

  public CompletionStage<Publisher<? extends Message>> get() {
    CompletableFuture<Publisher<? extends Message>> future = new CompletableFuture<>();
    server = vertx.createHttpServer();

    BehaviorProcessor<HttpServerRequest> processor = BehaviorProcessor.create();
    Publisher<? extends Message> publisher = ReactiveStreams.fromPublisher(processor)
      .flatMapCompletionStage(this::toMessage)
      .buildRs();
    server
      .requestHandler(req -> {
        if (req.path().equalsIgnoreCase("/health")) {
          req.response().setStatusCode(200).end(new JsonObject().put("status", "ok").encode());
        } else {
          processor.onNext(req);
        }
      })
      .listen(port, host, ar -> {
        if (ar.failed()) {
          future.completeExceptionally(ar.cause());
        } else {
          future.complete(publisher);
        }
      });
    return future;
  }

  public void stop() {
    server.close();
  }

  private CompletionStage<HttpMessage<byte[]>> toMessage(HttpServerRequest request) {
    HttpMessage.HttpMessageBuilder<byte[]> builder = HttpMessage.HttpMessageBuilder.create();
    builder.withMethod(request.method().name());
    builder.withUrl(request.path());
    MultiMap params = request.params();
    params.names().forEach(name -> builder.withQueryParameter(name, params.getAll(name)));
    MultiMap headers = request.headers();
    headers.names().forEach(name -> builder.withHeader(name, headers.getAll(name)));

    CompletableFuture<HttpMessage<byte[]>> future = new CompletableFuture<>();
    if (request.method() == HttpMethod.PUT || request.method() == HttpMethod.POST) {
      request.bodyHandler(buffer -> {
        builder.withPayload(buffer.getBytes());
        HttpMessage<byte[]> message = builder.build();
        future.complete(message);
      });
    } else {
      builder.withPayload(new byte[0]); // Empty.
      future.complete(builder.build());
    }
    request.response().setStatusCode(202).end();
    return future;
  }


}
