package io.smallrye.reactive.messaging.aws.sns;

import static io.smallrye.reactive.messaging.aws.sns.i18n.AwsSnsLogging.log;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;

public class SnsClientConfig {

    private final String topicName;
    private final String topicArn;
    private final Region region;
    private final String endpointOverride;
    private final String credentialsProviderClassName;

    public SnsClientConfig(final SnsConnectorOutgoingConfiguration config) {
        this.topicName = config.getTopic().orElse(config.getChannel());
        this.topicArn = config.getTopicArn().orElse(null);
        this.region = config.getRegion().map(Region::of).orElse(null);
        this.endpointOverride = config.getEndpointOverride().orElse(null);
        this.credentialsProviderClassName = config.getCredentialsProvider().orElse(null);
    }

    public boolean isComplete() {
        return topicName != null && topicArn != null;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getTopicArn() {
        return topicArn;
    }

    public Region getRegion() {
        if (region == null) {
            return DefaultAwsRegionProviderChain.builder().build().getRegion();
        }
        return region;
    }

    public String getEndpointOverride() {
        return endpointOverride;
    }

    @SuppressWarnings("unchecked")
    public AwsCredentialsProvider createCredentialsProvider() {
        final var className = credentialsProviderClassName == null
                ? "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
                : credentialsProviderClassName;
        try {
            final var clazz = (Class<? extends AwsCredentialsProvider>) Class.forName(className);
            final var method = clazz.getMethod("create");
            return (AwsCredentialsProvider) method.invoke(null);
        } catch (final Exception e) {
            log.failedToLoadAwsCredentialsProvider(e.getMessage());
            return DefaultCredentialsProvider.create();
        }
    }
}
