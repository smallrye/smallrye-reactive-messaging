package interceptors;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.IncomingInterceptor;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Message;

@Identifier("channel-a")
@ApplicationScoped
public class MyIncomingInterceptor implements IncomingInterceptor {

    @Override
    public Message<?> onMessage(Message<?> message) {
        return message.withPayload("changed " + message.getPayload());
    }

    @Override
    public void onMessageAck(Message<?> message) {
        // Called after message ack
    }

    @Override
    public void onMessageNack(Message<?> message, Throwable failure) {
        // Called after message nack
    }
}
