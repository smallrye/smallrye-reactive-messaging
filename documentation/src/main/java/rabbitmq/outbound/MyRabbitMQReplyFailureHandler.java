package rabbitmq.outbound;

import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.smallrye.reactive.messaging.rabbitmq.reply.ReplyFailureHandler;

@ApplicationScoped
@Identifier("my-reply-error")
public class MyRabbitMQReplyFailureHandler implements ReplyFailureHandler {

    @Override
    public Throwable handleReply(IncomingRabbitMQMessage<?> replyMessage) {
        var error = (String) replyMessage.getHeaders().get("REPLY_ERROR");
        if (error != null) {
            return (Throwable) new IllegalArgumentException(error);
        }
        return null;
    }
}
