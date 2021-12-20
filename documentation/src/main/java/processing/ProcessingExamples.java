package processing;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Uni;

public class ProcessingExamples {

    // <process-message>
    @Incoming("in")
    @Outgoing("out")
    public Message<String> processMessage(Message<Integer> in) {
        return in.withPayload(Integer.toString(in.getPayload()));
    }
    // </process-message>

    // <process-message-cs>
    @Incoming("in")
    @Outgoing("out")
    public CompletionStage<Message<String>> processMessageCS(Message<Integer> in) {
        CompletionStage<String> cs = invokeService(in.getPayload());
        return cs.thenApply(in::withPayload);
    }
    // </process-message-cs>

    // <process-message-uni>
    @Incoming("in")
    @Outgoing("out")
    public Uni<Message<String>> processMessageUni(Message<String> in) {
        return invokeService(in.getPayload())
                .map(in::withPayload);
    }
    // </process-message-uni>

    // <process-payload>
    @Incoming("in")
    @Outgoing("out")
    public String processPayload(int in) {
        return Integer.toString(in);
    }
    // </process-payload>

    // <process-payload-cs>
    @Incoming("in")
    @Outgoing("out")
    public CompletionStage<String> processPayloadCS(int in) {
        return invokeService(in);
    }
    // </process-payload-cs>

    // <process-payload-uni>
    @Incoming("in")
    @Outgoing("out")
    public Uni<String> processPayload(String in) {
        return invokeService(in);
    }
    // </process-payload-uni>

    private CompletionStage<String> invokeService(int param) {
        return CompletableFuture.completedFuture("ok");
    }

    private Uni<String> invokeService(String param) {
        return Uni.createFrom().item("ok");
    }

}
