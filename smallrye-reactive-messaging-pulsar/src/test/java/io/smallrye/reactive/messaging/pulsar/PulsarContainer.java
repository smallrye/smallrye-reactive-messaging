package io.smallrye.reactive.messaging.pulsar;

import org.testcontainers.containers.GenericContainer;

public class PulsarContainer extends GenericContainer<PulsarContainer> {

    public static final String PULSAR_IMAGE = "apachepulsar/pulsar:2.9.1";
    public static final int PULSAR_PORT = 6650;

    public PulsarContainer() {
        super(PULSAR_IMAGE);
        withExposedPorts(PULSAR_PORT, 8080);
        withCommand("bin/pulsar", "standalone");
    }

    public PulsarContainer withPort(final int fixedPort) {
        if (fixedPort <= 0) {
            throw new IllegalArgumentException("The fixed port must be greater than 0");
        }
        addFixedExposedPort(fixedPort, PULSAR_PORT);
        return self();
    }

    public String getClusterServiceUrl() {
        return String.format("pulsar://%s:%s", this.getHost(), this.getMappedPort(PULSAR_PORT));
    }
}
