package org.acme;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.reply.KafkaRequestReply;

@ApplicationScoped
public class KafkaRequestReplyMultiEmitter {

    @Inject
    @Channel("my-request")
    KafkaRequestReply<String, Integer> quoteRequest;

    public Multi<Integer> requestQuote(String request) {
        return quoteRequest.requestMulti(request).select().first(5);
    }
}
