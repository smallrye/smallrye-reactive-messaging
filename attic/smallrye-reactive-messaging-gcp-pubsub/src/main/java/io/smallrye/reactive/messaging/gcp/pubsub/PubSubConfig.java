package io.smallrye.reactive.messaging.gcp.pubsub;

import static io.smallrye.reactive.messaging.gcp.pubsub.i18n.PubSubMessages.msg;

import java.nio.file.Path;
import java.util.Objects;

public class PubSubConfig {
    // always required
    private final String projectId;
    private final String topic;

    private final Path credentialPath;

    private final String subscription;

    private final boolean mockPubSubTopics;
    private final String host;
    private final Integer port;

    public PubSubConfig(final String projectId, final String topic, final Path credentialPath, final boolean mockPubSubTopics,
            final String host, final Integer port) {
        this.projectId = Objects.requireNonNull(projectId, msg.mustNotBeNull("projectId"));
        this.topic = Objects.requireNonNull(topic, msg.mustNotBeNull("topic"));
        this.credentialPath = credentialPath;
        this.subscription = null;
        this.mockPubSubTopics = mockPubSubTopics;
        this.host = host;
        this.port = port;
    }

    public PubSubConfig(final String projectId, final String topic, final Path credentialPath, final String subscription,
            final boolean mockPubSubTopics, final String host, final Integer port) {
        this.projectId = Objects.requireNonNull(projectId, msg.mustNotBeNull("projectId"));
        this.topic = Objects.requireNonNull(topic, msg.mustNotBeNull("topic"));
        this.credentialPath = credentialPath;
        this.subscription = subscription;
        this.mockPubSubTopics = mockPubSubTopics;
        this.host = host;
        this.port = port;
    }

    public String getProjectId() {
        return projectId;
    }

    public String getTopic() {
        return topic;
    }

    public Path getCredentialPath() {
        return credentialPath;
    }

    public String getSubscription() {
        return subscription;
    }

    public boolean isMockPubSubTopics() {
        return mockPubSubTopics;
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PubSubConfig that = (PubSubConfig) o;
        return Objects.equals(projectId, that.projectId) &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(credentialPath, that.credentialPath) &&
                Objects.equals(subscription, that.subscription) &&
                mockPubSubTopics == that.mockPubSubTopics &&
                Objects.equals(host, that.host) &&
                Objects.equals(port, that.port);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projectId, topic, credentialPath, subscription, mockPubSubTopics, host, port);
    }

    @Override
    public String toString() {
        return "PubSubConfig[" +
                "projectId=" + projectId +
                ", topic=" + topic +
                ", credentialPath=" + credentialPath +
                ", subscription=" + subscription +
                ", mockPubSubTopics=" + mockPubSubTopics +
                ", host=" + host +
                ", port=" + port +
                ']';
    }
}
