package io.smallrye.reactive.messaging.spi;


import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.MessagingProvider;
import org.reactivestreams.Publisher;

import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public interface PublisherFactory {

  Class<? extends MessagingProvider> type();

  // TODO Would be nice to pass a Config object here.
  CompletionStage<Publisher<? extends Message>> createPublisher(Map<String, String> config);

}
