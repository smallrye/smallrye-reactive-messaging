package rabbitmq.outbound;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.rabbitmq.reply.RabbitMQRequestReply;

@ApplicationScoped
public class RabbitMQRequestReplyEmitter {

    @Inject
    @Channel("my-request")
    RabbitMQRequestReply<String, Integer> quoteRequest;

    public Uni<Integer> requestQuote(String request) {
        return quoteRequest.request(request);
    }
}
