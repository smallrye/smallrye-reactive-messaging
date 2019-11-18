package io.smallrye.reactive.messaging.jms.support;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.commons.io.FileUtils;

public class ArtemisHolder {

    private EmbeddedActiveMQ embedded;

    public Map<String, String> start() {
        try {
            FileUtils.deleteDirectory(Paths.get("./target/artemis").toFile());
            embedded = new EmbeddedActiveMQ();
            embedded.start();
        } catch (Exception e) {
            throw new RuntimeException("Could not start embedded ActiveMQ server", e);
        }
        return Collections.emptyMap();
    }

    public void stop() {
        try {
            embedded.stop();
        } catch (Exception e) {
            throw new RuntimeException("Could not stop embedded ActiveMQ server", e);
        }
    }
}
