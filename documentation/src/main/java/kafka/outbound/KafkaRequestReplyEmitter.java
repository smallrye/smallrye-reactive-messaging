package kafka.outbound;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.reply.KafkaRequestReply;

@ApplicationScoped
public class KafkaRequestReplyEmitter {

    @Inject
    @Channel("my-request")
    KafkaRequestReply<String, Integer> quoteRequest;

    public Uni<Integer> requestQuote(String request) {
        return quoteRequest.request(request);
    }
}
