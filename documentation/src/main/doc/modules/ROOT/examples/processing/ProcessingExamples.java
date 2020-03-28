package processing;

import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class ProcessingExamples {

    // tag::process-message[]
    @Incoming("in")
    @Outgoing("out")
    public Message<String> processMessage(Message<Integer> in) {
        return in.withPayload(Integer.toString(in.getPayload()));
    }
    // end::process-message[]

    // tag::process-message-cs[]
    @Incoming("in")
    @Outgoing("out")
    public CompletionStage<Message<String>> processMessageCS(Message<Integer> in) {
        CompletionStage<String> cs = invokeService(in.getPayload());
        return cs.thenApply(in::withPayload);
    }
    // end::process-message-cs[]

    // tag::process-message-uni[]
    @Incoming("in")
    @Outgoing("out")
    public Uni<Message<String>> processMessageUni(Message<String> in) {
        return invokeService(in.getPayload())
            .map(in::withPayload);
    }
    // end::process-message-uni[]

    // tag::process-payload[]
    @Incoming("in")
    @Outgoing("out")
    public String processPayload(int in) {
        return Integer.toString(in);
    }
    // end::process-payload[]

    // tag::process-payload-cs[]
    @Incoming("in")
    @Outgoing("out")
    public CompletionStage<String> processPayloadCS(int in) {
        return invokeService(in);
    }
    // end::process-payload-cs[]

    // tag::process-payload-uni[]
    @Incoming("in")
    @Outgoing("out")
    public Uni<String> processPayload(String in) {
        return invokeService(in);
    }
    // end::process-payload-uni[]


    private CompletionStage<String> invokeService(int param) {
        return CompletableFuture.completedFuture("ok");
    }

    private Uni<String> invokeService(String param) {
        return Uni.createFrom().item("ok");
    }


}
