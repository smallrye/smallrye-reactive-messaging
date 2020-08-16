package io.smallrye.reactive.messaging.ce.impl;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Map;

import io.smallrye.reactive.messaging.ce.CloudEventMetadata;
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;

/**
 * Default implementation of the {@link OutgoingCloudEventMetadata} interface.
 *
 * @param <T> the type of data
 */
public class DefaultOutgoingCloudEventMetadata<T> extends BaseCloudEventMetadata<T> implements OutgoingCloudEventMetadata<T> {

    public DefaultOutgoingCloudEventMetadata(String specVersion, String id, URI source, String type,
            String dataContentType, URI dataSchema, String subject, ZonedDateTime timestamp,
            Map<String, Object> extensions) {
        super(specVersion, id, source, type, dataContentType, dataSchema, subject, timestamp, extensions, null);
    }

    public DefaultOutgoingCloudEventMetadata(CloudEventMetadata<T> existing) {
        this(existing.getSpecVersion(), existing.getId(), existing.getSource(), existing.getType(),
                existing.getDataContentType().orElse(null), existing.getDataSchema().orElse(null),
                existing.getSubject().orElse(null),
                existing.getTimeStamp().orElse(null),
                existing.getExtensions());
    }

    @Override
    public T getData() {
        // OutgoingCloudEventMetadata cannot have data set.
        return null;
    }
}
