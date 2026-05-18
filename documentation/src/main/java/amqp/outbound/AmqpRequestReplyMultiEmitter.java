package amqp.outbound;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.amqp.reply.AmqpRequestReply;

@ApplicationScoped
public class AmqpRequestReplyMultiEmitter {

    @Inject
    @Channel("my-request")
    AmqpRequestReply<String, Integer> quoteRequest;

    public Multi<Integer> requestQuote(String request) {
        return quoteRequest.requestMulti(request).select().first(5);
    }
}
