package testing;

import io.smallrye.reactive.messaging.connectors.InMemoryConnector;

import java.util.HashMap;
import java.util.Map;

public class MyTestSetup {

    // tag::code[]
    public Map<String, String> start() {
        Map<String, String> env = new HashMap<>();
        env.putAll(InMemoryConnector.switchIncomingChannelsToInMemory("prices"));
        env.putAll(InMemoryConnector.switchOutgoingChannelsToInMemory("my-data-stream"));
        return env;
    }

    public void stop() {
        InMemoryConnector.clear();
    }
    // end::code[]

}
