package io.smallrye.reactive.messaging.spi;


import io.vertx.reactivex.core.Vertx;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.MessagingProvider;
import org.reactivestreams.Subscriber;

import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public interface SubscriberFactory {

  Class<? extends MessagingProvider> type();

  //TODO Would be nice to pass a config here.
  CompletionStage<Subscriber<? extends Message>> createSubscriber(Vertx vertx, Map<String, String> config);

}
