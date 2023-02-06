package io.smallrye.reactive.messaging.providers.helpers;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;
import jakarta.enterprise.inject.spi.Prioritized;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.providers.i18n.ProviderLogging;

public class CDIUtils {

    public static <T extends Prioritized> List<T> getSortedInstances(Instance<T> instances) {
        if (instances.isUnsatisfied()) {
            return Collections.emptyList();
        }

        return instances.stream().sorted(Comparator.comparingInt(Prioritized::getPriority))
                .collect(Collectors.toList());
    }

    public static <T> Instance<T> getInstanceById(Instance<T> instances, String identifier) {
        Instance<T> matching = instances.select(Identifier.Literal.of(identifier));
        if (matching.isUnsatisfied()) {
            // this `if` block should be removed when support for the `@Named` annotation is removed
            matching = instances.select(NamedLiteral.of(identifier));
            if (!matching.isUnsatisfied()) {
                ProviderLogging.log.deprecatedNamed();
            }
        }
        return matching;
    }

    public static <T> T getInstanceById(Instance<T> instances, String identifier, Supplier<T> defaultSupplier) {
        Instance<T> instanceById = getInstanceById(instances, identifier);
        if (instanceById.isUnsatisfied()) {
            return defaultSupplier.get();
        } else {
            return instanceById.get();
        }
    }
}
