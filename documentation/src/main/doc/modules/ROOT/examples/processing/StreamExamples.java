package processing;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

public class StreamExamples {

    // tag::processing-stream-message[]
    @Incoming("in")
    @Outgoing("out")
    public Multi<Message<String>> processMessageStream(Multi<Message<Integer>> stream) {
        return
            stream
                .onItem().produceUni(message ->
                invokeService(message.getPayload())
                    .onFailure().recoverWithItem("fallback")
                    .onItem().apply(message::withPayload)
            )
                .concatenate();

    }
    // end::processing-stream-message[]

    // tag::processing-stream-payload[]
    @Incoming("in")
    @Outgoing("out")
    public Multi<String> processPayloadStream(Multi<Integer> stream) {
        return
            stream
                .onItem().produceUni(payload ->
                invokeService(payload)
                    .onFailure().recoverWithItem("fallback")
            )
                .concatenate();

    }
    // end::processing-stream-payload[]

    private Uni<String> invokeService(int i) {
        return Uni.createFrom().item("ok " + i);
    }

}
