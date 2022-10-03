package io.smallrye.reactive.messaging.kafka.impl;

import java.util.Optional;

import io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgoingConfiguration;

/**
 * This class duplicate all configuration of the <code>KafkaConnectorOutgoingConfiguration</code> that needs to be
 * access at runtime (as opposed to configuration time), meaning all the items that are access for each message.
 *
 * Accessing configuration items via microprofile API is costly so we are better to cache them.
 */
public class RuntimeKafkaSinkConfiguration {
    private final int partition;
    private final Optional<String> key;
    private final Optional<String> cloudEventSubject;
    private final String propagateHeaders;
    private final Optional<String> cloudEventsDataContentType;
    private final Optional<String> cloudEventsDataSchema;
    private final Optional<String> cloudEventSource;
    private final Optional<String> cloudEventsType;
    private final Boolean cloudEventsInsertTimestamp;
    private final Boolean propagateRecordKey;
    private final Boolean tracingEnabled;

    private RuntimeKafkaSinkConfiguration(int partition, Optional<String> key, Optional<String> cloudEventSubject,
            String propagateHeaders, Optional<String> cloudEventsDataContentType, Optional<String> cloudEventsDataSchema,
            Optional<String> cloudEventSource, Optional<String> cloudEventsType, Boolean cloudEventsInsertTimestamp,
            Boolean propagateRecordKey, Boolean tracingEnabled) {
        this.partition = partition;
        this.key = key;
        this.cloudEventSubject = cloudEventSubject;
        this.propagateHeaders = propagateHeaders;
        this.cloudEventsDataContentType = cloudEventsDataContentType;
        this.cloudEventsDataSchema = cloudEventsDataSchema;
        this.cloudEventSource = cloudEventSource;
        this.cloudEventsType = cloudEventsType;
        this.cloudEventsInsertTimestamp = cloudEventsInsertTimestamp;
        this.propagateRecordKey = propagateRecordKey;
        this.tracingEnabled = tracingEnabled;
    }

    public static RuntimeKafkaSinkConfiguration buildFromConfiguration(KafkaConnectorOutgoingConfiguration configuration) {
        return new RuntimeKafkaSinkConfiguration(
                configuration.getPartition(),
                configuration.getKey(),
                configuration.getCloudEventsSubject(),
                configuration.getPropagateHeaders(),
                configuration.getCloudEventsDataContentType(),
                configuration.getCloudEventsDataSchema(),
                configuration.getCloudEventsSource(),
                configuration.getCloudEventsType(),
                configuration.getCloudEventsInsertTimestamp(),
                configuration.getPropagateRecordKey(),
                configuration.getTracingEnabled());
    }

    public int getPartition() {
        return partition;
    }

    public Optional<String> getKey() {
        return key;
    }

    public Optional<String> getCloudEventsSubject() {
        return cloudEventSubject;
    }

    public String getPropagateHeaders() {
        return propagateHeaders;
    }

    public Optional<String> getCloudEventsDataContentType() {
        return cloudEventsDataContentType;
    }

    public Optional<String> getCloudEventsDataSchema() {
        return cloudEventsDataSchema;
    }

    public Optional<String> getCloudEventsSource() {
        return cloudEventSource;
    }

    public Optional<String> getCloudEventsType() {
        return cloudEventsType;
    }

    public Boolean getCloudEventsInsertTimestamp() {
        return cloudEventsInsertTimestamp;
    }

    public Boolean getPropagateRecordKey() {
        return propagateRecordKey;
    }

    public Boolean getTracingEnabled() {
        return tracingEnabled;
    }
}
