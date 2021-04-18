package io.smallrye.reactive.messaging.eventbus;

import java.util.concurrent.CompletableFuture;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import io.smallrye.mutiny.Multi;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.eventbus.MessageConsumer;

class EventBusSource {

    private final String address;
    private final boolean ack;
    private final Vertx vertx;
    private final boolean broadcast;

    EventBusSource(Vertx vertx, VertxEventBusConnectorIncomingConfiguration config) {
        this.vertx = vertx;
        this.address = config.getAddress();
        this.broadcast = config.getBroadcast();
        this.ack = config.getUseReplyAsAck();
    }

    PublisherBuilder<? extends Message<?>> source() {
        MessageConsumer<Message<?>> consumer = vertx.eventBus().consumer(address);
        Multi<io.vertx.mutiny.core.eventbus.Message<Message<?>>> multi = consumer.toMulti();
        if (broadcast) {
            multi = multi.broadcast().toAllSubscribers();
        }
        return ReactiveStreams.fromPublisher(multi)
                .map(this::adapt);
    }

    private Message<?> adapt(io.vertx.mutiny.core.eventbus.Message<?> msg) {
        if (this.ack) {
            return new EventBusMessage<>(msg, () -> {
                msg.reply("OK");
                return CompletableFuture.completedFuture(null);
            });
        } else {
            return new EventBusMessage<>(msg, null);
        }
    }
}
