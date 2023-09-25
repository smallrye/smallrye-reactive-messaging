package io.smallrye.reactive.messaging.rabbitmq.fault;

import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnector;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnectorIncomingConfiguration;
import io.vertx.mutiny.core.Context;

/**
 * Implemented to provide message failure strategies.
 */
@Experimental("Experimental API")
public interface RabbitMQFailureHandler {

    /**
     * Identifiers of default failure strategies
     */
    interface Strategy {
        String FAIL = "fail";
        String ACCEPT = "accept";
        String REJECT = "reject";
        String REQUEUE = "requeue";
    }

    /**
     * Factory interface for {@link RabbitMQFailureHandler}
     */
    interface Factory {
        RabbitMQFailureHandler create(
                RabbitMQConnectorIncomingConfiguration config,
                RabbitMQConnector connector);
    }

    /**
     * Handle message failure.
     *
     * @param message the failed message
     * @param metadata additional nack metadata, may be {@code null}
     * @param context the {@link Context} in which the handling should be done
     * @param reason the reason for the failure
     * @param <V> message body type
     * @return a {@link CompletionStage}
     */
    <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message, Metadata metadata, Context context, Throwable reason);

}
