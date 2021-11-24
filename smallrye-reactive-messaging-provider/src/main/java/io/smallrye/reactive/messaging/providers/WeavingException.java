package io.smallrye.reactive.messaging.providers;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderMessages.msg;

import java.util.List;

public class WeavingException extends RuntimeException {

    public WeavingException(String source, String method, int number) {
        super(msg.weavingUnableToConnect(source, method, number));
    }

    /**
     * Used when a synchronous error is caught during the subscription
     *
     * @param source the source
     * @param cause the cause
     */
    public WeavingException(String source, Throwable cause) {
        super(msg.weavingSynchronousError(source), cause);
    }

    public WeavingException(List<String> sources, Throwable cause) {
        super(msg.weavingSynchronousError(sources.toString()), cause);
    }

    public WeavingException(String message) {
        super(message);
    }
}
