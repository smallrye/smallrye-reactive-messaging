package amqp.outbound;

import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.amqp.AmqpMessage;
import io.smallrye.reactive.messaging.amqp.reply.ReplyFailureHandler;

@ApplicationScoped
@Identifier("my-reply-error")
public class MyAmqpReplyFailureHandler implements ReplyFailureHandler {

    @Override
    public Throwable handleReply(AmqpMessage<?> replyMessage) {
        var properties = replyMessage.getApplicationProperties();
        if (properties != null) {
            String error = properties.getString("REPLY_ERROR");
            if (error != null) {
                return new IllegalArgumentException(error);
            }
        }
        return null;
    }
}
