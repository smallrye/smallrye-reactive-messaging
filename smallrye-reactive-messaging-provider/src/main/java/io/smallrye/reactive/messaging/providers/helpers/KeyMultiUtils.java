package io.smallrye.reactive.messaging.providers.helpers;

import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.CDI;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.MediatorConfiguration;
import io.smallrye.reactive.messaging.keyed.KeyValueExtractor;
import io.smallrye.reactive.messaging.keyed.KeyedMulti;
import io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions;

public class KeyMultiUtils {

    private KeyMultiUtils() {
        // Avoid direct instantiation.
    }

    public static Multi<KeyedMulti<?, ?>> convertToKeyedMulti(Multi<? extends Message<?>> multi,
            Instance<KeyValueExtractor> extractors, MediatorConfiguration configuration) {
        Type keyType = configuration.getKeyType();
        Type valueType = configuration.getValueType();

        if (keyType == null) {
            throw ProviderExceptions.ex.failedToExtractKeyType(configuration.methodAsString());
        }
        if (valueType == null) {
            throw ProviderExceptions.ex.failedToExtractValueType(configuration.methodAsString());
        }

        List<KeyValueExtractor> sortedExtractors = CDIUtils.getSortedInstances(extractors);

        if (configuration.getKeyed() == null) {
            AtomicReference<KeyValueExtractor> reference = new AtomicReference<>();
            return multi
                    .invoke(m -> {
                        if (reference.get() == null) {
                            KeyValueExtractor found = findExtractor(m, keyType, valueType, sortedExtractors, configuration);
                            reference.compareAndSet(null, found);
                        }
                    })
                    .group().by(m -> reference.get().extractKey(m, keyType), m -> reference.get().extractValue(m, valueType))
                    .map(gm -> new DefaultKeyedMulti<>(gm.key(), gm));
        } else {
            KeyValueExtractor extractor = findExtractor(configuration.getKeyed());
            return multi
                    .group().by(m -> extractor.extractKey(m, keyType), m -> extractor.extractValue(m, valueType))
                    .map(gm -> new DefaultKeyedMulti<>(gm.key(), gm));
        }
    }

    private static KeyValueExtractor findExtractor(Message<?> message, Type keyType, Type valueType,
            List<KeyValueExtractor> extractors, MediatorConfiguration configuration) {
        return extractors.stream().filter(ext -> ext.canExtract(message, keyType, valueType))
                .findAny().orElseThrow(() -> ProviderExceptions.ex.noMatchingKeyValueExtractor(configuration.methodAsString()));
    }

    private static KeyValueExtractor findExtractor(Class<? extends KeyValueExtractor> clazz) {
        // Throw an unsatisfied exception if not found
        return CDI.current().select(clazz).get();
    }
}
