package io.smallrye.reactive.messaging.aws.sqs;

import java.util.Objects;
import java.util.Optional;

import org.eclipse.microprofile.config.Config;
import software.amazon.awssdk.regions.Region;

public class SqsConfig {

    private final Config config;

    public SqsConfig(Config config) {
        this.config = config;
    }

    public String getQueueName() {
        return config.getOptionalValue("queue", String.class)
                .orElseThrow(() -> new IllegalArgumentException("queue must be set"));
    }

    public int getWaitTimeSeconds() {
        return config.getOptionalValue("waitTimeSeconds", Integer.class).orElse(20);
    }

    public int getMaxNumberOfMessages() {
        return config.getOptionalValue("maxNumberOfMessages", Integer.class).orElse(10);
    }

    public Optional<Region> getRegion() {
        var result = config.getOptionalValue("region", String.class);
        if (result.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Region.of(result.get()));
        } catch (IllegalArgumentException e) {
            // TODO: LOG
            return Optional.empty();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var that = (SqsConfig) o;
        return getMaxNumberOfMessages() == that.getMaxNumberOfMessages() &&
                getWaitTimeSeconds() == that.getWaitTimeSeconds() &&
                Objects.equals(getEndpointOverride(), that.getEndpointOverride()) &&
                Objects.equals(getRegion(), that.getRegion()) &&
                Objects.equals(getQueueName(), that.getQueueName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getEndpointOverride(), getRegion(), getQueueName(), getMaxNumberOfMessages(), getWaitTimeSeconds());
    }

    public Optional<String> getEndpointOverride() {
        return config.getOptionalValue("endpointOverride", String.class);
    }
}
