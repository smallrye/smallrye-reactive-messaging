package io.smallrye.reactive.messaging.spi;


import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.MessagingProvider;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;

import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public interface IncomingConnectorFactory {

  Class<? extends MessagingProvider> type();

  PublisherBuilder<? extends Message> getPublisherBuilder(Config config);

}
