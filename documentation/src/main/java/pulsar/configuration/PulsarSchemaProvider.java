package pulsar.configuration;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;

import io.smallrye.common.annotation.Identifier;

@ApplicationScoped
public class PulsarSchemaProvider {

    @Produces
    @Identifier("user-schema")
    Schema<User> userSchema = Schema.AVRO(User.class);

    @Produces
    @Identifier("a-channel")
    Schema<KeyValue<Integer, User>> keyValueSchema() {
        return Schema.KeyValue(Schema.INT32, Schema.JSON(User.class), KeyValueEncodingType.SEPARATED);
    }

    public static class User {
        String name;
        int age;

    }
}
