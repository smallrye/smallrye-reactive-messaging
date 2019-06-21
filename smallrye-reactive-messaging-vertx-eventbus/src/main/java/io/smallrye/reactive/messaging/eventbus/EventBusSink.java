package io.smallrye.reactive.messaging.eventbus;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.reactivex.core.Vertx;

public class EventBusSink {

    private final String address;
    private final Vertx vertx;
    private final boolean publish;
    private final boolean expectReply;
    private final String codec;
    private final int timeout;

    EventBusSink(Vertx vertx, Config config) {
        this.vertx = Objects.requireNonNull(vertx, "Vert.x instance must not be `null`");
        this.address = config.getOptionalValue("address", String.class)
                .orElseThrow(() -> new IllegalArgumentException("`address` must be set"));
        this.publish = config.getOptionalValue("publish", Boolean.class).orElse(false);
        this.expectReply = config.getOptionalValue("expect-reply", Boolean.class).orElse(false);

        if (this.publish && this.expectReply) {
            throw new IllegalArgumentException("Cannot enable `publish` and `expect-reply` at the same time");
        }

        this.codec = config.getOptionalValue("codec", String.class).orElse(null);
        this.timeout = config.getOptionalValue("timeout", Integer.class).orElse(-1);
    }

    SubscriberBuilder<? extends Message<?>, Void> sink() {
        DeliveryOptions options = new DeliveryOptions();
        if (this.codec != null) {
            options.setCodecName(this.codec);
        }
        if (this.timeout != -1) {
            options.setSendTimeout(this.timeout);
        }

        return ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(msg -> {
                    CompletableFuture<Message> future = new CompletableFuture<>();
                    // TODO support getting an EventBusMessage as message.
                    if (!this.publish) {
                        if (expectReply) {
                            vertx.eventBus().send(address, msg.getPayload(), options, ar -> {
                                if (ar.failed()) {
                                    future.completeExceptionally(ar.cause());
                                } else {
                                    future.complete(msg);
                                }
                            });
                        } else {
                            vertx.eventBus().send(address, msg.getPayload(), options);
                            future.complete(msg);
                        }
                    } else {
                        vertx.eventBus().publish(address, msg.getPayload(), options);
                        future.complete(msg);
                    }
                    return future;
                })
                .ignore();
    }

}
