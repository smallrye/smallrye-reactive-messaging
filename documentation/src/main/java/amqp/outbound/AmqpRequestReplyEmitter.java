package amqp.outbound;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.amqp.reply.AmqpRequestReply;

@ApplicationScoped
public class AmqpRequestReplyEmitter {

    @Inject
    @Channel("my-request")
    AmqpRequestReply<String, Integer> quoteRequest;

    public Uni<Integer> requestQuote(String request) {
        return quoteRequest.request(request);
    }
}
