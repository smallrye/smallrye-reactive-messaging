package messages;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;

public class MetadataWithPayloadChainExamples {

    // <chain>
    @Outgoing("source")
    public Multi<Message<Integer>> generate() {
        return Multi.createFrom().range(0, 10)
                .map(i -> Message.of(i, Metadata.of(new MyMetadata("author", "me"))));
    }

    @Incoming("source")
    @Outgoing("sink")
    public int increment(int i) {
        return i + 1;
    }

    @Outgoing("sink")
    public CompletionStage<Void> consume(Message<Integer> in) {
        Optional<MyMetadata> metadata = in.getMetadata(MyMetadata.class);
        return in.ack();
    }
    // </chain>

}
