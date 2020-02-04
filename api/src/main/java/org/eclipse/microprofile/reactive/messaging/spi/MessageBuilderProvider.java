package org.eclipse.microprofile.reactive.messaging.spi;

import org.eclipse.microprofile.config.spi.ConfigProviderResolver;
import org.eclipse.microprofile.reactive.messaging.MessageBuilder;

import java.util.Iterator;
import java.util.ServiceLoader;

public abstract class MessageBuilderProvider {

    private static volatile MessageBuilderProvider instance = null;

    public abstract <T> MessageBuilder<T> newBuilder();

    public static MessageBuilderProvider instance() {
        if (instance == null) {
            synchronized (ConfigProviderResolver.class) {
                if (instance != null) {
                    return instance;
                }
                instance = loadSpi(MessageBuilderProvider.class.getClassLoader());
            }
        }
        return instance;
    }

    private static MessageBuilderProvider loadSpi(ClassLoader classLoader) {
        ServiceLoader<MessageBuilderProvider> sl = ServiceLoader.load(
            MessageBuilderProvider.class, classLoader);
        final Iterator<MessageBuilderProvider> iterator = sl.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        throw new IllegalStateException(
            "No ConfigProviderResolver implementation found!");
    }

}
