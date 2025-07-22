package io.smallrye.reactive.messaging.amqp.fault;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;

import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import io.smallrye.reactive.messaging.amqp.AmqpMessage;
import io.smallrye.reactive.messaging.amqp.ConnectionHolder;
import io.vertx.mutiny.core.Context;

public class AmqpFailStop implements AmqpFailureHandler {

    private final String channel;
    private final BiConsumer<String, Throwable> reportFailure;

    public AmqpFailStop(String channel, BiConsumer<String, Throwable> reportFailure) {
        this.reportFailure = reportFailure;
        this.channel = channel;
    }

    @Override
    public <V> CompletionStage<Void> handle(AmqpMessage<V> msg, Context context, Throwable reason) {
        // We mark the message as rejected and fail.
        log.nackedFailMessage(channel);
        reportFailure.accept(channel, reason);
        return ConnectionHolder.runOnContextAndReportFailure(context, msg, reason, io.vertx.mutiny.amqp.AmqpMessage::rejected);
    }
}
