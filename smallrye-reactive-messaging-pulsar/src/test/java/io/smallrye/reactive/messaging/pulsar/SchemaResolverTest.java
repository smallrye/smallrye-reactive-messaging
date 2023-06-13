package io.smallrye.reactive.messaging.pulsar;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.pulsar.base.SingletonInstance;
import io.smallrye.reactive.messaging.pulsar.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

class SchemaResolverTest {

    @Test
    void testSchemaFromName() {
        SchemaResolver schemaResolver = new SchemaResolver(UnsatisfiedInstance.instance());
        Schema<?> schema = schemaResolver.getSchema(new PulsarConnectorIncomingConfiguration(new MapBasedConfig()
                .with("schema", "INT32")
                .with("channel-name", "channel")));
        assertThat(schema.getSchemaInfo().getName()).isEqualTo("INT32");
        assertThat(schema).hasSameClassAs(Schema.INT32);
    }

    @Test
    void testSchemaFromIdentifier() {
        SchemaResolver schemaResolver = new SchemaResolver(new SingletonInstance<>("my-schema", Schema.STRING));
        Schema<?> schema = schemaResolver.getSchema(new PulsarConnectorIncomingConfiguration(new MapBasedConfig()
                .with("schema", "my-schema")
                .with("channel-name", "channel")));
        assertThat(schema.getSchemaInfo().getName()).isEqualTo("String");
        assertThat(schema).isEqualTo(Schema.STRING);
    }

    @Test
    void testSchemaFromChannelName() {
        SchemaResolver schemaResolver = new SchemaResolver(new SingletonInstance<>("channel", Schema.STRING));
        Schema<?> schema = schemaResolver.getSchema(new PulsarConnectorIncomingConfiguration(new MapBasedConfig()
                .with("channel-name", "channel")));
        assertThat(schema.getSchemaInfo().getName()).isEqualTo("String");
        assertThat(schema).isEqualTo(Schema.STRING);
    }

    @Test
    void testSchemaFromDefault() {
        SchemaResolver schemaResolver = new SchemaResolver(new SingletonInstance<>("my-schema", Schema.STRING));
        Schema<?> inSchema = schemaResolver.getSchema(new PulsarConnectorIncomingConfiguration(new MapBasedConfig()
                .with("channel-name", "channel")));
        assertThat(inSchema).hasSameClassAs(Schema.AUTO_CONSUME());
        Schema<?> outSchema = schemaResolver.getSchema(new PulsarConnectorOutgoingConfiguration(new MapBasedConfig()
                .with("channel-name", "channel")));
        assertThat(outSchema).hasSameClassAs(Schema.AUTO_PRODUCE_BYTES());
    }
}
