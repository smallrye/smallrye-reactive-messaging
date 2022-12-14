package interceptors;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.OutgoingInterceptor;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;

@Identifier("channel-a")
@ApplicationScoped
public class MyInterceptor implements OutgoingInterceptor {

    @Override
    public Message<?> onMessage(Message<?> message) {
        return message.withPayload("changed " + message.getPayload());
    }

    @Override
    public void onMessageAck(Message<?> message) {
        message.getMetadata(OutgoingMessageMetadata.class)
                .ifPresent(m -> m.getResult());
    }

    @Override
    public void onMessageNack(Message<?> message, Throwable failure) {

    }
}
