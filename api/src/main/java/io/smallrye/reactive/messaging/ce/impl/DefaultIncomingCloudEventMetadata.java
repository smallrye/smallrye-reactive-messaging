package io.smallrye.reactive.messaging.ce.impl;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Map;

import io.smallrye.reactive.messaging.ce.CloudEventMetadata;
import io.smallrye.reactive.messaging.ce.IncomingCloudEventMetadata;

/**
 * Default implementation of the {@link IncomingCloudEventMetadata} interface
 *
 * @param <T> the type of data
 */
public class DefaultIncomingCloudEventMetadata<T> extends BaseCloudEventMetadata<T> implements IncomingCloudEventMetadata<T> {

    public DefaultIncomingCloudEventMetadata(String specVersion, String id, URI source, String type,
            String dataContentType, URI dataSchema, String subject, ZonedDateTime timestamp,
            Map<String, Object> extensions, T data) {
        super(specVersion, id, source, type, dataContentType, dataSchema, subject, timestamp, extensions, data);
        validate();
    }

    public DefaultIncomingCloudEventMetadata(CloudEventMetadata<T> existing) {
        this(existing.getSpecVersion(), existing.getId(), existing.getSource(), existing.getType(),
                existing.getDataContentType().orElse(null), existing.getDataSchema().orElse(null),
                existing.getSubject().orElse(null),
                existing.getTimeStamp().orElse(null),
                existing.getExtensions(), existing.getData());
        validate();
    }

}
