package ack;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import java.util.concurrent.CompletableFuture;

public class MessageAckExamples {

    // tag::with-payload[]
    @Incoming("in")
    @Outgoing("out")
    public Message<Integer> process(Message<Integer> in) {
        // The acknowledgement is forwarded, when the consumer
        // acknowledges the message, `in` will be acknowledged
        return in.withPayload(in.getPayload() + 1);
    }
    // end::with-payload[]


    public void creation() {
        // tag::message-creation[]
        Message<String> message = Message.of("hello", () -> {
            // called when the consumer acknowledges the message

            // return a CompletionStage completed when the
            // acknowledgment of the created message is
            // completed.
            // For immediate ack use:
            return CompletableFuture.completedFuture(null);

        });
        // end::message-creation[]
    }


    // tag::process[]
    @Incoming("in")
    @Outgoing("out")
    public Message<Integer> processAndProduceNewMessage(Message<Integer> in) {
        // The acknowledgement is forwarded, when the consumer
        // acknowledges the message, `in` will be acknowledged
        return Message.of(in.getPayload() + 1,
            () -> {
                // Called when the consumer acknowledges the message
                // ...
                // Don't forget to acknowledge the incoming message:
                return in.ack();
            });
    }
    // end::process[]

}
