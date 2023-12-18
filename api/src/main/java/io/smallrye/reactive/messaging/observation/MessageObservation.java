package io.smallrye.reactive.messaging.observation;

import java.time.Duration;

import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * The message observation contract
 */
public interface MessageObservation {

    /**
     * @return the channel name of the message
     */
    String getChannel();

    /**
     * @return the creation time of the message in system nanos
     */
    long getCreationTime();

    /**
     * @return the completion time of the message in system nanos
     */
    long getCompletionTime();

    /**
     *
     * @return the duration between creation and the completion time, null if message processing is not completed
     */
    Duration getCompletionDuration();

    /**
     *
     * @return {@code true} if the message processing is completed with acknowledgement or negative acknowledgement
     */
    boolean isDone();

    /**
     * @return the negative acknowledgement reason
     */
    Throwable getReason();

    /**
     * Notify the observation of acknowledgement event
     *
     */
    void onMessageAck(Message<?> message);

    /**
     * Notify the observation of negative acknowledgement event
     *
     * @param reason the reason of the negative acknowledgement
     */
    void onMessageNack(Message<?> message, Throwable reason);
}
