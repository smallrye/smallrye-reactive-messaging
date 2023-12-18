package kafka.outbound;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.reply.KafkaRequestReply;

@ApplicationScoped
public class KafkaRequestReplyMessageEmitter {

    @Inject
    @Channel("my-request")
    KafkaRequestReply<String, Integer> quoteRequest;

    public Uni<Message<Integer>> requestMessage(String request) {
        return quoteRequest.request(Message.of(request)
                .addMetadata(OutgoingKafkaRecordMetadata.builder()
                        .withKey(request)
                        .build()));
    }
}
