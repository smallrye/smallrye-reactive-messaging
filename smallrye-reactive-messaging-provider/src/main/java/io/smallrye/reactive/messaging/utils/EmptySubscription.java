package io.smallrye.reactive.messaging.utils;

import org.reactivestreams.Subscription;

/**
 * A {@link Subscription implementation} ignoring everything.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class EmptySubscription implements Subscription {
  @Override
  public void request(long n) {
    // Ignored.
  }

  @Override
  public void cancel() {
    // Ignored.
  }
}
