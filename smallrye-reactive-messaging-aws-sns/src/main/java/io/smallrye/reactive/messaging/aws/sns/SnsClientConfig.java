package io.smallrye.reactive.messaging.aws.sns;

/**
 * A configuration POJO class to create SnsClient.
 *
 * @author iabughosh
 */
class SnsClientConfig {

    private final String host;
    private final boolean mockSnsTopic;

    /**
     * Main constructor.
     *
     * @param host the host
     * @param mockSnsTopic whether we are in mock mode
     */
    SnsClientConfig(String host, boolean mockSnsTopic) {
        this.host = host;
        this.mockSnsTopic = mockSnsTopic;
    }

    String getHost() {
        return host;
    }

    boolean isMockSnsTopic() {
        return mockSnsTopic;
    }
}
