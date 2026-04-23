package io.smallrye.reactive.messaging.amqp.reply;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
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

    private static final Duration DEFAULT_TIMEOUT = Duration.ofMillis(3000);

    private final Uni<AmqpSender> requestSender;
    private final Uni<AmqpReceiver> replyReceiver;
    private final Map<String, UniEmitter<AmqpMessage>> pendingRequests = new ConcurrentHashMap<>();
    private final String idPrefix = UUID.randomUUID().toString().substring(0, 8) + "-";
    private final AtomicLong requestId = new AtomicLong(0);
    private final String replyToAddress;
    private final Duration timeout;
    private final Cancellable receiveCancellation;
    private volatile AmqpSender actualSender;
    private volatile AmqpReceiver actualReceiver;
    private volatile boolean closed = false;

    public AmqpDirectRequestReply(AmqpConnection connection, String address, String linkName) {
        this(connection, address, linkName, address + "-client-reply-to", linkName + "-client-reply-to");
    }

    public AmqpDirectRequestReply(AmqpConnection connection, String address, String linkName,
            String replyToAddress, String replyToLinkName) {
        this(connection, address, new AmqpSenderOptions().setLinkName(linkName),
                replyToAddress, new AmqpReceiverOptions().setLinkName(replyToLinkName), DEFAULT_TIMEOUT);
    }

    public AmqpDirectRequestReply(AmqpConnection connection, String address, AmqpSenderOptions senderOptions,
            String replyToAddress, AmqpReceiverOptions receiverOptions) {
        this(connection, address, senderOptions, replyToAddress, receiverOptions, DEFAULT_TIMEOUT);
    }

    public AmqpDirectRequestReply(AmqpConnection connection, String address, AmqpSenderOptions senderOptions,
            String replyToAddress, AmqpReceiverOptions receiverOptions, Duration timeout) {
        this.requestSender = connection.createSender(address, senderOptions)
                .onItem().invoke(s -> actualSender = s)
                .memoize().until(() -> closed);
        this.replyToAddress = replyToAddress;
        this.timeout = timeout;
        this.replyReceiver = connection.createReceiver(replyToAddress, receiverOptions)
                .onItem().invoke(r -> actualReceiver = r)
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
                }, this::failAllPending,
                        () -> failAllPending(
                                new AmqpRequestReplyTimeoutException("AmqpDirectRequestReply receiver completed")));
    }

    public Uni<AmqpMessage> request(AmqpMessage message) {
        if (closed) {
            return Uni.createFrom().failure(new IllegalStateException("AmqpDirectRequestReply is closed"));
        }
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
                .ifNoItem().after(timeout)
                .failWith(() -> new AmqpRequestReplyTimeoutException(correlationId))
                .onTermination().invoke(() -> pendingRequests.remove(correlationId));
    }

    private String generateMessageId() {
        return idPrefix + requestId.incrementAndGet();
    }

    private void failAllPending(Throwable failure) {
        log.requestReplyConsumerFailure("direct-reply", failure);
        var iterator = pendingRequests.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            iterator.remove();
            entry.getValue().fail(failure);
        }
    }

    public void close() {
        closed = true;
        receiveCancellation.cancel();
        failAllPending(new AmqpRequestReplyTimeoutException("AmqpDirectRequestReply closed"));
        if (actualSender != null) {
            actualSender.closeAndForget();
        }
        if (actualReceiver != null) {
            actualReceiver.closeAndForget();
        }
    }
}
