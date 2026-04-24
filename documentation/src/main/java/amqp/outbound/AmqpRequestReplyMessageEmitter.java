package amqp.outbound;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.amqp.reply.AmqpRequestReply;

@ApplicationScoped
public class AmqpRequestReplyMessageEmitter {

    @Inject
    @Channel("my-request")
    AmqpRequestReply<String, Integer> quoteRequest;

    public Uni<Message<Integer>> requestMessage(String request) {
        return quoteRequest.request(Message.of(request));
    }
}
