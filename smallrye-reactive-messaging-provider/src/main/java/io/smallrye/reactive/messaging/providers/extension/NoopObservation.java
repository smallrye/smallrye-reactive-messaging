package io.smallrye.reactive.messaging.providers.extension;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.observation.ReactiveMessagingObservation;

public class NoopObservation implements ReactiveMessagingObservation {

    @Override
    public MessageObservation onNewMessage(String channel, Message<?> message) {
        return NoopMessageObservation.INSTANCE;
    }

    private static class NoopMessageObservation extends MessageObservation {

        public static NoopMessageObservation INSTANCE = new NoopMessageObservation();

        private NoopMessageObservation() {
            super(null, null);
        }

        @Override
        public void onProcessingStart() {
            // NOOP
        }

        @Override
        public void onAckOrNack(Throwable nackReason) {
            // NOOP
        }
    }
}
