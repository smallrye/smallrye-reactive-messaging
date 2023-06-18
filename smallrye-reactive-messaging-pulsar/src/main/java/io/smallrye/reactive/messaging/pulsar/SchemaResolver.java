package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging.log;

import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;
import io.smallrye.reactive.messaging.providers.helpers.Validation;

@ApplicationScoped
public class SchemaResolver {

    private final Instance<Schema<?>> schemas;

    @Inject
    public SchemaResolver(@Any Instance<Schema<?>> schemas) {
        this.schemas = schemas;
    }

    public Schema<?> getSchema(PulsarConnectorIncomingConfiguration ic) {
        return getSchema(ic, Schema.AUTO_CONSUME());
    }

    public Schema<?> getSchema(PulsarConnectorOutgoingConfiguration oc) {
        return getSchema(oc, Schema.AUTO_PRODUCE_BYTES());
    }

    private Schema<?> getSchema(PulsarConnectorCommonConfiguration configuration, Schema<?> defaultSchema) {
        Optional<String> schemaName = configuration.getSchema();
        if (schemaName.isPresent()) {
            try {
                return Schema.getSchema(SchemaInfo.builder()
                        .type(SchemaType.valueOf(schemaName.get()))
                        .build());
            } catch (Exception ignored) {
                log.primitiveSchemaNotFound(schemaName.get(), configuration.getChannel());
            }
        }
        String schemaIdentifier = schemaName.orElse(configuration.getChannel());
        return CDIUtils.getInstanceById(schemas, schemaIdentifier, () -> {
            log.schemaProviderNotFound(schemaIdentifier, configuration.getChannel(), defaultSchema.toString());
            return defaultSchema;
        });
    }

    public static String getSchemaName(Schema<?> schema) {
        SchemaInfo schemaInfo = schema.getSchemaInfo();
        if (schemaInfo == null || Validation.isBlank(schemaInfo.getName())) {
            return schema.getClass().getSimpleName();
        } else {
            return schemaInfo.getName();
        }
    }
}
