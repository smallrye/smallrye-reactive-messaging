package emitter;

import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;

public class ChannelExamples {

    // <channel>
    @Inject
    @Channel("my-channel")
    Multi<String> streamOfPayloads;

    @Inject
    @Channel("my-channel")
    Multi<Message<String>> streamOfMessages;

    @Inject
    @Channel("my-channel")
    Publisher<String> publisherOfPayloads;

    @Inject
    @Channel("my-channel")
    Publisher<Message<String>> publisherOfMessages;
    // </channel>
}
