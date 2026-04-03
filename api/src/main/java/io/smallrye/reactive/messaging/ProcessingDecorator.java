package io.smallrye.reactive.messaging;

import jakarta.enterprise.inject.spi.Prioritized;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Uni;

/**
 * SPI interface for decorating message processing at the invocation level.
 * <p>
 * Implementations can decorate the {@link Uni} representing the method invocation result,
 * enabling cross-cutting concerns such as timeouts, metrics, or circuit breaking
 * to be applied per-message.
 * <p>
 * Similar to {@link PublisherDecorator} and {@link SubscriberDecorator} which operate
 * at the stream level, this decorator operates at the individual message processing level.
 * <p>
 * The {@link #decorate(MediatorConfiguration)} method is called once per mediator during
 * initialization. It receives the mediator configuration and returns a
 * {@link ProcessingInterceptor} to apply per-message, or {@code null} if this decorator
 * does not apply to the given mediator.
 * <p>
 * Implementations are CDI beans discovered at startup. Multiple decorators are chained
 * in priority order (via {@link Prioritized}).
 */
@Experimental("SmallRye only feature")
public interface ProcessingDecorator extends Prioritized {

    int DEFAULT_PRIORITY = 100;

    /**
     * Called once per mediator during initialization to obtain a per-message interceptor.
     * <p>
     * The mediator configuration provides access to incoming channel names, acknowledgment
     * strategy, method signature, blocking flag, and other mediator-level concerns.
     *
     * @param configuration the mediator configuration
     * @return a {@link ProcessingInterceptor} to apply per-message, or {@code null} if this
     *         decorator does not apply to the given mediator
     */
    ProcessingInterceptor decorate(MediatorConfiguration configuration);

    @Override
    default int getPriority() {
        return DEFAULT_PRIORITY;
    }

    /**
     * A per-message interceptor returned by {@link ProcessingDecorator#decorate(MediatorConfiguration)}.
     */
    @FunctionalInterface
    interface ProcessingInterceptor {
        /**
         * Intercepts the processing {@link Uni} for a single message.
         * <p>
         * The returned Uni replaces the original processing Uni. Implementations must
         * ensure that the returned Uni eventually completes (or fails) to avoid stalling
         * the processing pipeline.
         *
         * @param processing the Uni representing the method invocation
         * @param message the message being processed
         * @return the decorated Uni
         */
        Uni<?> intercept(Uni<?> processing, Message<?> message);
    }
}
