package io.smallrye.reactive.messaging.http;

import io.smallrye.reactive.messaging.http.converters.Serializer;
import io.smallrye.reactive.messaging.spi.ConfigurationHelper;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.ext.web.client.HttpRequest;
import io.vertx.reactivex.ext.web.client.WebClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Subscriber;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class HttpSink {

  private static final Logger LOGGER = LogManager.getLogger(HttpSink.class);

  private final String url;
  private final WebClient client;
  private final String converterClass;
  private final Subscriber<? extends Message> subscriber;

  public HttpSink(Vertx vertx, ConfigurationHelper config) {
    WebClientOptions options = new WebClientOptions(config.asJsonObject());
    url = config.getOrDie("url");
    client = WebClient.create(vertx, options);
    converterClass = config.get("converter");

    subscriber = ReactiveStreams.<Message>builder()
      .flatMapCompletionStage(m -> send(m).thenApply(v -> m))
      .ignore()
      .build();
  }

  CompletionStage<Void> send(Message message) {
    if (message instanceof HttpMessage) {
      Serializer<Object> serializer = Serializer.lookup(message.getPayload(), converterClass);
      HttpRequest request = toRequest((HttpMessage) message);
      return serializer.convert(message.getPayload())
        .thenCompose(buffer -> invoke(request, buffer));
    } else {
      Object payload = message.getPayload();
      Serializer<Object> serializer = Serializer.lookup(payload, converterClass);
      return serializer.convert(payload)
        .thenCompose(this::invoke);
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
          throw new IllegalArgumentException("Invalid method: " + message.getMethod() + ", only PUT and POST are supported");
      }
    }

    message.getHeaders().forEach((k, v) -> v.forEach(item -> request.headers().add(k, item)));
    message.getQuery().forEach(request::addQueryParam);

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
            LOGGER.debug("HTTP request POST {}  has failed with status code: {}, body is: {}", url, resp.statusCode(),
              resp.body() != null ? resp.body().toString() : "NO CONTENT");
            future.completeExceptionally(new Exception("HTTP request POST " + url + " has not returned a valid status: " + resp.statusCode()));
          }
        },
        future::completeExceptionally
      );
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
            LOGGER.debug("HTTP request POST {}  has failed with status code: {}, body is: {}", url, resp.statusCode(),
              resp.body() != null ? resp.body().toString() : "NO CONTENT");
            future.completeExceptionally(new Exception("HTTP request POST " + url + " has not returned a valid status: " + resp.statusCode()));
          }
        },
        future::completeExceptionally
      );
    return future;
  }

  public CompletionStage<Subscriber<? extends Message>> get() {
    return CompletableFuture.completedFuture(subscriber);
  }
}
