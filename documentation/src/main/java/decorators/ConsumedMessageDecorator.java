package decorators;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.PublisherDecorator;

// <code>
@ApplicationScoped
public class ConsumedMessageDecorator implements PublisherDecorator {

    private final Map<String, AtomicLong> counters = new HashMap<>();

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, String channelName,
            boolean isConnector) {
        if (isConnector) {
            AtomicLong counter = new AtomicLong();
            counters.put(channelName, counter);
            return publisher.onItem().invoke(counter::incrementAndGet);
        } else {
            return publisher;
        }
    }

    @Override
    public int getPriority() {
        return 10;
    }

    public long getMessageCount(String channel) {
        return counters.get(channel).get();
    }
}
// </code>
