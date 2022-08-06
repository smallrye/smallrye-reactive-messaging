package io.smallrye.reactive.messaging;

import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Metadata injected for holding the result of outgoing connector's transmission operation
 * <p>
 * Connector implementations are responsible for searching for this metadata on the outgoing message
 * and setting the transmission result on that metadata object.
 * <p>
 * Implementations of {@link OutgoingInterceptor} can access the result
 * on {@link OutgoingInterceptor#onMessageAck(Message)} callback.
 *
 * @param <T> type of the transmission result
 */
public class OutgoingMessageMetadata<T> {

    private T result;

    public static void setResultOnMessage(Message<?> message, Object result) {
        message.getMetadata(OutgoingMessageMetadata.class).ifPresent(m -> m.setResult(result));
    }

    public void setResult(T result) {
        this.result = result;
    }

    public T getResult() {
        return result;
    }
}
