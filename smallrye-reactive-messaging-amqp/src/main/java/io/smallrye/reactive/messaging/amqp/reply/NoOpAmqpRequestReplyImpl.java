package io.smallrye.reactive.messaging.amqp.reply;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.providers.extension.MutinyEmitterImpl;

/**
 * A no-op implementation of {@link AmqpRequestReply} used when the channel is not backed by the AMQP connector
 * (e.g. in testing with an in-memory connector).
 * <p>
 * Requests are sent through the standard emitter path. Replies are resolved via a configurable
 * reply function set through {@link #setReplyFunction(Function)}.
 */
public class NoOpAmqpRequestReplyImpl<Req, Rep> extends MutinyEmitterImpl<Req>
        implements AmqpRequestReply<Req, Rep> {

    private volatile Function<Req, Rep> replyFunction;

    public NoOpAmqpRequestReplyImpl(EmitterConfiguration config, long defaultBufferSize) {
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
                                        + "'. Call setReplyFunction() on the NoOpAmqpRequestReplyImpl.");
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
                                        + "'. Call setReplyFunction() on the NoOpAmqpRequestReplyImpl.");
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
