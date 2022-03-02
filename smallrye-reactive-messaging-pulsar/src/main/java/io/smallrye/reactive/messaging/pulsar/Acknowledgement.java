package io.smallrye.reactive.messaging.pulsar;

import java.util.concurrent.CompletionStage;

interface Acknowledgement {
    CompletionStage<Void> ack(org.apache.pulsar.client.api.Message<?> message);

    CompletionStage<Void> nack(org.apache.pulsar.client.api.Message<?> message, Throwable reason);
}
