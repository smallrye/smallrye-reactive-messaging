package io.smallrye.reactive.messaging.amqp.reply;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.mutiny.subscription.UniEmitter;
import io.vertx.amqp.AmqpReceiverOptions;
import io.vertx.amqp.AmqpSenderOptions;
import io.vertx.mutiny.amqp.AmqpConnection;
import io.vertx.mutiny.amqp.AmqpMessage;
import io.vertx.mutiny.amqp.AmqpReceiver;
import io.vertx.mutiny.amqp.AmqpSender;

/**
 * A simple request-reply mechanism using AMQP.
 */
public class AmqpDirectRequestReply {

    private final Uni<AmqpSender> requestSender;
    private final Uni<AmqpReceiver> replyReceiver;
    private final Map<String, UniEmitter<AmqpMessage>> pendingRequests = new ConcurrentHashMap<>();
    private final AtomicLong requestId = new AtomicLong(0);
    private final String replyToAddress;
    private final Cancellable receiveCancellation;
    private volatile boolean closed = false;

    public AmqpDirectRequestReply(AmqpConnection connection, String address, String linkName) {
        this(connection, address, linkName, address + "-client-reply-to", linkName + "-client-reply-to");
    }

    public AmqpDirectRequestReply(AmqpConnection connection, String address, String linkName,
            String replyToAddress, String replyToLinkName) {
        this(connection, address, new AmqpSenderOptions().setLinkName(linkName),
                replyToAddress, new AmqpReceiverOptions().setLinkName(replyToLinkName));
    }

    public AmqpDirectRequestReply(AmqpConnection connection, String address, AmqpSenderOptions senderOptions,
            String replyToAddress, AmqpReceiverOptions receiverOptions) {
        this.requestSender = connection.createSender(address, senderOptions).memoize().until(() -> closed);
        this.replyToAddress = replyToAddress;
        this.replyReceiver = connection.createReceiver(replyToAddress, receiverOptions)
                .memoize().until(() -> closed);
        this.receiveCancellation = replyReceiver
                .onItem().transformToMulti(AmqpReceiver::toMulti)
                .subscribe().with(message -> {
                    String correlationId = message.correlationId();
                    if (correlationId != null) {
                        UniEmitter<AmqpMessage> emitter = pendingRequests.remove(correlationId);
                        if (emitter != null) {
                            emitter.complete(message);
                        } else {
                            log.requestReplyMessageIgnored("direct-reply", correlationId);
                        }
                    }
                }, failure -> log.requestReplyConsumerFailure("direct-reply", failure));
    }

    public Uni<AmqpMessage> request(AmqpMessage message) {
        String correlationId = generateMessageId();
        AmqpMessage toSend = AmqpMessage.create(message)
                .id(correlationId)
                .replyTo(replyToAddress)
                .build();
        return Uni.createFrom().<AmqpMessage> emitter(em -> {
            pendingRequests.put(correlationId, (UniEmitter<AmqpMessage>) em);
            requestSender.flatMap(s -> s.sendWithAck(toSend))
                    .subscribe().with(
                            unused -> {
                            },
                            failure -> {
                                pendingRequests.remove(correlationId);
                                em.fail(failure);
                            });
        })
                .ifNoItem().after(Duration.ofMillis(3000))
                .failWith(() -> new AmqpRequestReplyTimeoutException(correlationId))
                .onTermination().invoke(() -> pendingRequests.remove(correlationId));
    }

    private String generateMessageId() {
        return "" + requestId.incrementAndGet();
    }

    public void close() {
        closed = true;
        receiveCancellation.cancel();
        var iterator = pendingRequests.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            iterator.remove();
            entry.getValue().fail(new AmqpRequestReplyTimeoutException(entry.getKey()));
        }
        requestSender.subscribe().with(AmqpSender::closeAndForget);
        replyReceiver.subscribe().with(AmqpReceiver::closeAndForget);
    }
}
