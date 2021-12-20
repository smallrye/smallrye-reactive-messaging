package skip;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;

public class StreamSkip {

    // <skip>
    @Incoming("in")
    @Outgoing("out-1")
    public Multi<String> processPayload(String s) {
        if (s.equalsIgnoreCase("skip")) {
            return Multi.createFrom().empty();
        }
        return Multi.createFrom().item(s.toUpperCase());
    }

    @Incoming("in")
    @Outgoing("out-2")
    public Multi<Message<String>> processMessage(Message<String> m) {
        String s = m.getPayload();
        if (s.equalsIgnoreCase("skip")) {
            return Multi.createFrom().empty();
        }
        return Multi.createFrom().item(m.withPayload(s.toUpperCase()));
    }

    @Incoming("in")
    @Outgoing("out-3")
    public Multi<String> processPayloadStream(Multi<String> stream) {
        return stream
                .select().where(s -> !s.equalsIgnoreCase("skip"))
                .onItem().transform(String::toUpperCase);
    }

    @Incoming("in")
    @Outgoing("out-4")
    public Multi<Message<String>> processMessageStream(Multi<Message<String>> stream) {
        return stream
                .select().where(m -> !m.getPayload().equalsIgnoreCase("skip"))
                .onItem().transform(m -> m.withPayload(m.getPayload().toUpperCase()));
    }
    // </skip>

}
