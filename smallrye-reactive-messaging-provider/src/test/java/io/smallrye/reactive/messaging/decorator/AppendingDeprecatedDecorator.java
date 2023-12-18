package io.smallrye.reactive.messaging.decorator;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class AppendingDeprecatedDecorator implements io.smallrye.reactive.messaging.providers.PublisherDecorator {

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, String channelName) {
        return publisher.map(m -> this.appendString(m, channelName));
    }

    private Message<?> appendString(Message<?> message, String string) {
        if (message.getPayload() instanceof String) {
            String payload = (String) message.getPayload();
            return Message.of(payload + "-" + string, metadata -> message.ack(metadata));
        } else {
            return message;
        }
    }

}
