package io.smallrye.reactive.messaging.spi;


import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.MessagingProvider;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public interface OutgoingConnectorFactory {

  Class<? extends MessagingProvider> type();

  SubscriberBuilder<? extends Message, Void> getSubscriberBuilder(Config config);

}
