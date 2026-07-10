package io.smallrye.reactive.messaging.rabbitmq.og.reply;

import java.util.Map;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.EmitterType;

@Experimental("Experimental API")
public interface RabbitMQRequestReply<Req, Rep> extends EmitterType {

    String REPLY_TIMEOUT_KEY = "reply.timeout";

    String REPLY_CORRELATION_ID_HANDLER_KEY = "reply.correlation-id.handler";

    String DEFAULT_CORRELATION_ID_HANDLER = "uuid";

    String REPLY_FAILURE_HANDLER_KEY = "reply.failure.handler";

    Uni<Rep> request(Req request);

    Uni<Message<Rep>> request(Message<Req> request);

    Multi<Rep> requestMulti(Req request);

    Multi<Message<Rep>> requestMulti(Message<Req> request);

    Map<CorrelationId, PendingReply> getPendingReplies();

    void complete();
}
