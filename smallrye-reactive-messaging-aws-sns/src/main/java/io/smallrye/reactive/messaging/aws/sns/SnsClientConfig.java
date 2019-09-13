package io.smallrye.reactive.messaging.aws.sns;

/**
 * A configuration POJO class to create SnsClient.
 * 
 * @author iabughosh
 * @version 1.0.4
 *
 */
public class SnsClientConfig {

    private final String host;
    private final boolean mockSnsTopic;

    /**
     * Main constructor.
     * 
     * @param host
     * @param mockSnsTopic
     */
    public SnsClientConfig(String host, boolean mockSnsTopic) {
        this.host = host;
        this.mockSnsTopic = mockSnsTopic;
    }

    public String getHost() {
        return host;
    }

    public boolean isMockSnsTopic() {
        return mockSnsTopic;
    }
}
