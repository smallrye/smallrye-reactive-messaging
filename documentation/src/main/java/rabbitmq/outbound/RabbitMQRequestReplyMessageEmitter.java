package rabbitmq.outbound;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.rabbitmq.reply.RabbitMQRequestReply;

@ApplicationScoped
public class RabbitMQRequestReplyMessageEmitter {

    @Inject
    @Channel("my-request")
    RabbitMQRequestReply<String, Integer> quoteRequest;

    public Uni<Message<Integer>> requestMessage(String request) {
        return quoteRequest.request(Message.of(request));
    }
}
