package processing;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

public class StreamExamples {

    // <processing-stream-message>
    @Incoming("in")
    @Outgoing("out")
    public Multi<Message<String>> processMessageStream(Multi<Message<Integer>> stream) {
        return stream
                .onItem().transformToUni(message -> invokeService(message.getPayload())
                        .onFailure().recoverWithItem("fallback")
                        .onItem().transform(message::withPayload))
                .concatenate();

    }
    // </processing-stream-message>

    // <processing-stream-payload>
    @Incoming("in")
    @Outgoing("out")
    public Multi<String> processPayloadStream(Multi<Integer> stream) {
        return stream
                .onItem().transformToUni(payload -> invokeService(payload)
                        .onFailure().recoverWithItem("fallback"))
                .concatenate();

    }
    // </processing-stream-payload>

    private Uni<String> invokeService(int i) {
        return Uni.createFrom().item("ok " + i);
    }

}
