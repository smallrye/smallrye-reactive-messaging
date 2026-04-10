package rabbitmq.outbound;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.rabbitmq.reply.RabbitMQRequestReply;

@ApplicationScoped
public class RabbitMQRequestReplyMultiEmitter {

    @Inject
    @Channel("my-request")
    RabbitMQRequestReply<String, Integer> quoteRequest;

    public Multi<Integer> requestQuote(String request) {
        return quoteRequest.requestMulti(request).select().first(5);
    }
}
