package io.smallrye.reactive.messaging.providers.helpers;

import static io.smallrye.reactive.messaging.providers.helpers.CDIUtils.getSortedInstances;

import java.lang.reflect.Type;
import java.util.function.Function;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.DefauiltKeyMetadata;
import io.smallrye.reactive.messaging.MessageKeyValueExtractor;

public class KeyExtractorUtils {

    private KeyExtractorUtils() {
        // Avoid direct instantiation.
    }

    public static <K, V> Function<Message<V>, Tuple2<K, V>> extractKeyValueFunction(
            Instance<MessageKeyValueExtractor> keyExtractors,
            Type injectedTableKeyType, Type injectedTableValueType) {
        return new Function<>() {

            MessageKeyValueExtractor actual;

            @Override
            public Tuple2<K, V> apply(Message<V> o) {
                //noinspection ConstantConditions - it can be `null`
                if (injectedTableKeyType == null || injectedTableValueType == null) {
                    return null;
                }

                if (actual != null) {
                    // Use the cached converter.
                    K extractKey = (K) actual.extractKey(o, injectedTableKeyType);
                    V extractValue = (V) actual.extractValue(o, injectedTableValueType);
                    return Tuple2.of(extractKey, extractValue);
                } else {
                    // Lookup and cache
                    for (MessageKeyValueExtractor ext : getSortedInstances(keyExtractors)) {
                        if (ext.canExtract(o, injectedTableKeyType, injectedTableValueType)) {
                            actual = ext;
                            K extractKey = (K) actual.extractKey(o, injectedTableKeyType);
                            V extractValue = (V) actual.extractValue(o, injectedTableValueType);
                            return Tuple2.of(extractKey, extractValue);
                        }
                    }
                    // No key extractor found
                    return Tuple2.of(o.getMetadata(DefauiltKeyMetadata.class)
                            .map(m -> (K) m.getMessageKey())
                            .orElse(null), o.getPayload());
                }
            }
        };
    }
}
