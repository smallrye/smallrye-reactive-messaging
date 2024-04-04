package io.smallrye.reactive.messaging.aws.sqs;

import static io.smallrye.reactive.messaging.aws.sqs.i18n.AwsSqsLogging.log;

import java.util.Objects;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;

public class SqsClientConfig {

    private final String queueName;
    private final String queueUrl;
    private final Region region;
    private final String endpointOverride;
    private final String credentialsProviderClassName;

    public SqsClientConfig(SqsConnectorCommonConfiguration config) {
        this.queueName = config.getQueue().orElse(config.getChannel());
        this.queueUrl = config.getQueueUrl().orElse(null);
        this.region = config.getRegion().map(Region::of).orElse(null);
        this.endpointOverride = config.getEndpointOverride().orElse(null);
        this.credentialsProviderClassName = config.getCredentialsProvider().orElse(null);
    }

    public boolean isComplete() {
        return queueName != null && region != null;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    public String getEndpointOverride() {
        return endpointOverride;
    }

    public Region getRegion() {
        return region;
    }

    public String getCredentialsProviderClassName() {
        return credentialsProviderClassName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var that = (SqsClientConfig) o;
        return Objects.equals(endpointOverride, that.endpointOverride) &&
                Objects.equals(region, that.region) &&
                Objects.equals(queueName, that.queueName) &&
                Objects.equals(queueUrl, that.queueUrl) &&
                Objects.equals(credentialsProviderClassName, that.credentialsProviderClassName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpointOverride, region, queueName, queueUrl, credentialsProviderClassName);
    }

    public AwsCredentialsProvider createCredentialsProvider() {
        var className = credentialsProviderClassName == null
                ? "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
                : credentialsProviderClassName;
        try {
            var clazz = (Class<? extends AwsCredentialsProvider>) Class.forName(className);
            var method = clazz.getMethod("create");
            return (AwsCredentialsProvider) method.invoke(null);
        } catch (Exception e) {
            log.failedToLoadAwsCredentialsProvider(e.getMessage());
            return DefaultCredentialsProvider.create();
        }
    }
}
