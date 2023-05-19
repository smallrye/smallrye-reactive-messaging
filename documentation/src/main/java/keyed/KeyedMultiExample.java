package keyed;

import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.keyed.KeyedMulti;

public class KeyedMultiExample {

    // <code>
    @Incoming("in")
    @Outgoing("out")
    public Multi<String> reshape(KeyedMulti<String, String> multi) {
        // Called once per key and receive the stream of value for that specific key
        String key = multi.key();
        return multi.onItem().scan(AtomicInteger::new, (i, s) -> {
            i.incrementAndGet();
            return i;
        })
                .map(i -> Integer.toString(i.get()));
    }
    // </code>

}
