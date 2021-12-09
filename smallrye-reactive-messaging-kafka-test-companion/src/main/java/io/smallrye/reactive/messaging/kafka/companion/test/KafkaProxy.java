package io.smallrye.reactive.messaging.kafka.companion.test;

import eu.rekawek.toxiproxy.Proxy;

/**
 * Test containers Toxiproxy doesn't give access to underlying Proxy object
 */
public class KafkaProxy {
    public final Proxy toxi;
    public final String containerIpAddress;
    public final int proxyPort;
    public final int originalProxyPort;

    protected KafkaProxy(Proxy toxi, String containerIpAddress, int proxyPort, int originalProxyPort) {
        this.toxi = toxi;
        this.containerIpAddress = containerIpAddress;
        this.proxyPort = proxyPort;
        this.originalProxyPort = originalProxyPort;
    }

    public String getProxyBootstrapServers() {
        return String.format("PLAINTEXT://%s:%s", this.containerIpAddress, this.proxyPort);
    }

}
