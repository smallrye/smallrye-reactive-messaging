package testing;

import java.util.HashMap;
import java.util.Map;

import io.smallrye.reactive.messaging.memory.TestingConnector;

public class MyTestSetup {

    // <code>
    public Map<String, String> start() {
        Map<String, String> env = new HashMap<>();
        env.putAll(TestingConnector.switchIncomingChannelsToTesting("prices"));
        env.putAll(TestingConnector.switchOutgoingChannelsToTesting("my-data-stream"));
        return env;
    }

    public void stop() {
        TestingConnector.clear();
    }
    // </code>

}
