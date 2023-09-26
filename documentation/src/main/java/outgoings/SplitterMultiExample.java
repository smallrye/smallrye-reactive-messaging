package outgoings;

import java.util.Objects;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.split.MultiSplitter;

public class SplitterMultiExample {

    // <code>
    enum Caps {
        ALL_CAPS,
        ALL_LOW,
        MIXED
    }

    @Incoming("in")
    @Outgoing("sink1")
    @Outgoing("sink2")
    @Outgoing("sink3")
    public MultiSplitter<String, Caps> reshape(Multi<String> in) {
        return in.split(Caps.class, s -> {
            if (Objects.equals(s, s.toLowerCase())) {
                return Caps.ALL_LOW;
            } else if (Objects.equals(s, s.toUpperCase())) {
                return Caps.ALL_CAPS;
            } else {
                return Caps.MIXED;
            }
        });
    }
    // </code>

}
