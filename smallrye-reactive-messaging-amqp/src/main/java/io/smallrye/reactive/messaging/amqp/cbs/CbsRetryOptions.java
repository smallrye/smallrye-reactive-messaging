package io.smallrye.reactive.messaging.amqp.cbs;

import static java.time.Duration.ofSeconds;

import io.smallrye.mutiny.Uni;

public record CbsRetryOptions(int maxRetries, long retryDelay) {

    <T> Uni<T> apply(Uni<T> uni) {
        return uni.onFailure().retry().withBackOff(ofSeconds(1), ofSeconds(retryDelay)).atMost(maxRetries);
    }
}
