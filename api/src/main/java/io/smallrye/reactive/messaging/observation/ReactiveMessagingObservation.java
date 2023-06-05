package io.smallrye.reactive.messaging.observation;

import org.eclipse.microprofile.reactive.messaging.Message;

public interface ReactiveMessagingObservation {

    MessageObservation onNewMessage(String channel, Message<?> message);

    class MessageObservation {

        protected final String channel;
        protected final Message<?> message;
        protected final long creationTime;
        protected volatile long processing = -1;
        protected volatile long completion = -1;
        protected volatile boolean succeed;

        public MessageObservation(String channel, Message<?> message) {
            this.channel = channel;
            this.message = message;
            this.creationTime = System.nanoTime();
        }

        public String channel() {
            return channel;
        }

        public Message<?> message() {
            return message;
        }

        public long creationTime() {
            return creationTime;
        }

        public void onProcessingStart() {
            this.processing = System.nanoTime();
        }

        public void onAckOrNack(boolean succeed) {
            this.completion = System.nanoTime();
            this.succeed = succeed;
        }

        public long processingStartTime() {
            return processing;
        }

        public long processingEndTime() {
            return completion;
        }

        public boolean succeeded() {
            return succeed;
        }

    }

}
