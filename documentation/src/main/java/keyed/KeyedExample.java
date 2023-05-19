package keyed;

import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.keyed.Keyed;
import io.smallrye.reactive.messaging.keyed.KeyedMulti;

public class KeyedExample {

    // <code>
    @Incoming("in")
    @Outgoing("out")
    public Multi<String> reshape(
            @Keyed(KeyValueExtractorFromPayload.class) KeyedMulti<String, String> multi) {
        return multi.onItem().scan(AtomicInteger::new, (i, s) -> {
            i.incrementAndGet();
            return i;
        }).map(i -> Integer.toString(i.get()));
    }
    // </code>
}
