package io.smallrye.reactive.messaging.aws.serialization;

import static io.smallrye.reactive.messaging.aws.i18n.AwsExceptions.ex;

import java.util.Map;
import java.util.function.Function;

import jakarta.enterprise.inject.AmbiguousResolutionException;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.smallrye.reactive.messaging.providers.i18n.ProviderLogging;

public class SerializationResolver {

    public static Serializer resolveSerializer(Instance<Serializer> messageSerializer,
            String name, String channel,
            JsonMapping jsonMapping) {
        return resolve(messageSerializer, name, defaultSerializer(jsonMapping),
                count -> ex.unableToFindSerializer(name, channel, count));
    }

    private static Serializer defaultSerializer(JsonMapping jsonMapping) {
        return payload -> {
            if (jsonMapping != null) {
                if (payload instanceof String) {
                    return (String) payload;
                }
                return jsonMapping.toJson(payload);
            } else {
                return String.valueOf(payload);
            }
        };
    }

    public static Deserializer resolveDeserializer(Instance<Deserializer> messageDeserializer,
            String name, String channel,
            JsonMapping jsonMapping) {
        return resolve(messageDeserializer, name, defaultDeserializer(jsonMapping),
                count -> ex.unableToFindDeserializer(name, channel, count));
    }

    private static Deserializer defaultDeserializer(JsonMapping jsonMapping) {
        return payload -> {
            if (jsonMapping != null) {
                return jsonMapping.fromJson(payload, Map.class);
            } else {
                return String.valueOf(payload);
            }
        };
    }

    private static <T> T resolve(Instance<T> instances, String name,
            T defaultValue,
            Function<Integer, AmbiguousResolutionException> ambiguous) {
        Instance<T> instance = instances.select(Identifier.Literal.of(name));

        if (instance.isUnsatisfied()) {
            // this `if` block should be removed when support for the `@Named` annotation is removed
            instance = instances.select(NamedLiteral.of(name));
            if (!instance.isUnsatisfied()) {
                ProviderLogging.log.deprecatedNamed();
            }
        }

        if (instance.isUnsatisfied()) {
            return defaultValue;
        } else if (instance.stream().count() > 1) {
            throw ambiguous.apply((int) instance.stream().count());
        } else {
            return instance.get();
        }
    }
}
