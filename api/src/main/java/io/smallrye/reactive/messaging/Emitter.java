package io.smallrye.reactive.messaging;

import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.annotations.Channel;

/**
 * Interface used to feed a channel from an <em>imperative</em> piece of code.
 * <p>
 * Instances are injected using:
 *
 * <pre>
 * &#64;Inject
 * &#64;Channel("my-channel")
 * Emitter&lt;String&gt; emitter;
 * </pre>
 * <p>
 * You can inject emitter sending payload or
 * {@link org.eclipse.microprofile.reactive.messaging.Message Messages}, using:
 * <ul>
 * <li>{@link #send(Object)} to send payload. The returned {@link CompletionStage} is completed (with {@code null}
 * when the message is acknowledged</li>
 * <li>{@link #send(Message)} to send message.</li>
 * </ul>
 *
 * <p>
 * The name of the channel (given in the {@link Channel Channel annotation})
 * indicates which channel is fed. It must match the name used in a method using
 * {@link org.eclipse.microprofile.reactive.messaging.Incoming @Incoming} or an
 * {@code outgoing} channel configured in the application configuration.
 *
 * This class supersedes {@link io.smallrye.reactive.messaging.annotations.Emitter}.
 *
 * @param <T> type of payload.
 */
public interface Emitter<T> {

    /**
     * Sends a payload to the channel.
     *
     * @param msg the <em>thing</em> to send, must not be {@code null}
     * @return the {@code CompletionStage}, which will be completed with {@code null} when the message is acknowledged.
     * @throws IllegalStateException if the stream has been cancelled or terminated.
     */
    CompletionStage<Void> send(T msg);

    /**
     * Sends a message to the channel.
     *
     * @param msg the <em>Message</em> to send, must not be {@code null}
     * @throws IllegalStateException if the stream has been cancelled or terminated.
     */
    <M extends Message<? extends T>> void send(M msg);

    /**
     * Completes the stream.
     * This method sends the completion signal to the channel, no messages can be sent once this method is called.
     */
    void complete();

    /**
     * Propagates an error in the stream.
     * This methods sends an error signal to the channel, no messages can be sent once this method is called.
     *
     * @param e the exception, must not be {@code null}
     */
    void error(Exception e);

    /**
     * @return {@code true} if the emitter has been terminated or the subscription cancelled.
     */
    boolean isCancelled();

    /**
     * @return {@code true} if the subscriber accepts messages, {@code false} otherwise.
     */
    boolean isRequested();

}
