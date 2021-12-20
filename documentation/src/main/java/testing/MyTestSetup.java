package testing;

import java.util.HashMap;
import java.util.Map;

import io.smallrye.reactive.messaging.providers.connectors.InMemoryConnector;

public class MyTestSetup {

    // <code>
    public Map<String, String> start() {
        Map<String, String> env = new HashMap<>();
        env.putAll(InMemoryConnector.switchIncomingChannelsToInMemory("prices"));
        env.putAll(InMemoryConnector.switchOutgoingChannelsToInMemory("my-data-stream"));
        return env;
    }

    public void stop() {
        InMemoryConnector.clear();
    }
    // </code>

}
