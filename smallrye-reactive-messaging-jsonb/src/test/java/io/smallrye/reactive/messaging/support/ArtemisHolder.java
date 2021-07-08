package io.smallrye.reactive.messaging.support;

import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.commons.io.FileUtils;

import java.nio.file.Paths;

class ArtemisHolder {

    private EmbeddedActiveMQ embedded;

    void start() {
        try {
            FileUtils.deleteDirectory(Paths.get("./target/artemis").toFile());
            embedded = new EmbeddedActiveMQ();
            embedded.start();
        } catch (Exception e) {
            throw new IllegalStateException("Could not start embedded ActiveMQ server", e);
        }
    }

    void stop() {
        try {
            embedded.stop();
        } catch (Exception e) {
            throw new IllegalStateException("Could not stop embedded ActiveMQ server", e);
        }
    }
}
