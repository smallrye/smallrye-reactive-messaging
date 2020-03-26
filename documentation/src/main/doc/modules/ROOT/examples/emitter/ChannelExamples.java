package emitter;

import io.smallrye.mutiny.Multi;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;

import javax.inject.Inject;

public class ChannelExamples {

    // tag::channel[]
    @Inject @Channel("my-channel") Multi<String> streamOfPayloads;

    @Inject @Channel("my-channel") Multi<Message<String>> streamOfMessages;

    @Inject @Channel("my-channel") Publisher<String> publisherOfPayloads;

    @Inject @Channel("my-channel") Publisher<Message<String>> publisherOfMessages;
    // end::channel[]
}
