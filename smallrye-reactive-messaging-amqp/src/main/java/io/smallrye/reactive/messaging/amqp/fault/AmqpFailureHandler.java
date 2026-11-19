package io.smallrye.reactive.messaging.amqp.fault;

import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.reactive.messaging.amqp.AmqpConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.amqp.AmqpMessage;
import io.vertx.mutiny.core.Context;

@Experimental("Experimental API")
public interface AmqpFailureHandler {

    interface Strategy {
        String FAIL = "fail";
        String ACCEPT = "accept";
        String RELEASE = "release";
        String REJECT = "reject";
        String MODIFIED_FAILED = "modified-failed";
        String MODIFIED_FAILED_UNDELIVERABLE_HERE = "modified-failed-undeliverable-here";
    }

    interface Factory {
        AmqpFailureHandler create(AmqpConnectorIncomingConfiguration config,
                BiConsumer<String, Throwable> reportFailure);
    }

    <V> CompletionStage<Void> handle(AmqpMessage<V> message, Context context, Throwable reason, Metadata metadata);

}
