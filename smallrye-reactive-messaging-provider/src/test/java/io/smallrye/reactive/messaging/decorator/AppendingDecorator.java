package io.smallrye.reactive.messaging.decorator;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;

import io.smallrye.reactive.messaging.PublisherDecorator;

@ApplicationScoped
public class AppendingDecorator implements PublisherDecorator {

    @Override
    public PublisherBuilder<? extends Message> decorate(PublisherBuilder<? extends Message> publisher, String channelName) {
        return publisher.map((m) -> this.appendString(m, channelName));
    }

    private Message appendString(Message message, String string) {
        if (message.getPayload() instanceof String) {
            String payload = (String) message.getPayload();
            return Message.of(payload + "-" + string, message::ack);
        } else {
            return message;
        }
    }

}
