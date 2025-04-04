package io.smallrye.reactive.messaging.aws.sns;

import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.reactive.messaging.OutgoingInterceptor;

@ApplicationScoped
public class LogFailureInterceptor implements OutgoingInterceptor {

    @Override
    public void onMessageAck(final org.eclipse.microprofile.reactive.messaging.Message<?> message) {
    }

    @Override
    public void onMessageNack(final org.eclipse.microprofile.reactive.messaging.Message<?> message,
            final Throwable failure) {
        failure.printStackTrace();
    }
}
