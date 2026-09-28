package io.smallrye.reactive.messaging.rabbitmq.reply;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.providers.extension.MutinyEmitterImpl;

/**
 * A no-op implementation of {@link RabbitMQRequestReply} used when the channel is not backed by the RabbitMQ connector
 * (e.g. in testing with an in-memory connector).
 * <p>
 * Requests are sent through the standard emitter path. Replies are resolved via a configurable
 * reply function set through {@link #setReplyFunction(Function)}.
 */
public class NoOpRabbitMQRequestReplyImpl<Req, Rep> extends MutinyEmitterImpl<Req>
        implements RabbitMQRequestReply<Req, Rep> {

    private volatile Function<Req, Rep> replyFunction;

    public NoOpRabbitMQRequestReplyImpl(EmitterConfiguration config, long defaultBufferSize) {
        super(config, defaultBufferSize);
    }

    public void setReplyFunction(Function<Req, Rep> replyFunction) {
        this.replyFunction = replyFunction;
    }

    @Override
    public Uni<Rep> request(Req request) {
        return sendMessage(Message.of(request))
                .map(unused -> {
                    Function<Req, Rep> fn = replyFunction;
                    if (fn == null) {
                        throw new IllegalStateException(
                                "No reply function configured for channel '" + name
                                        + "'. Call setReplyFunction() on the NoOpRabbitMQRequestReplyImpl.");
                    }
                    return fn.apply(request);
                });
    }

    @Override
    public Uni<Message<Rep>> request(Message<Req> request) {
        return sendMessage(request)
                .map(unused -> {
                    Function<Req, Rep> fn = replyFunction;
                    if (fn == null) {
                        throw new IllegalStateException(
                                "No reply function configured for channel '" + name
                                        + "'. Call setReplyFunction() on the NoOpRabbitMQRequestReplyImpl.");
                    }
                    return Message.of(fn.apply(request.getPayload()));
                });
    }

    @Override
    public Multi<Rep> requestMulti(Req request) {
        return Multi.createFrom().uni(request(request));
    }

    @Override
    public Multi<Message<Rep>> requestMulti(Message<Req> request) {
        return Multi.createFrom().uni(request(request));
    }

    @Override
    public Map<CorrelationId, PendingReply> getPendingReplies() {
        return Collections.emptyMap();
    }

    @Override
    public void complete() {
        // no-op
    }
}
