package io.smallrye.reactive.messaging.amqp.fault;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;

import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.amqp.AmqpConnector;
import io.smallrye.reactive.messaging.amqp.AmqpMessage;
import io.smallrye.reactive.messaging.amqp.ConnectionHolder;
import io.vertx.mutiny.core.Context;

public class AmqpFailStop implements AmqpFailureHandler {

    private final String channel;
    private final AmqpConnector connector;

    public AmqpFailStop(AmqpConnector connector, String channel) {
        this.connector = connector;
        this.channel = channel;
    }

    @Override
    public <V> CompletionStage<Void> handle(AmqpMessage<V> msg, Context context, Throwable reason) {
        // We mark the message as rejected and fail.
        log.nackedFailMessage(channel);
        connector.reportFailure(channel, reason);
        return ConnectionHolder.runOnContextAndReportFailure(context, msg, reason, io.vertx.mutiny.amqp.AmqpMessage::rejected);
    }
}
