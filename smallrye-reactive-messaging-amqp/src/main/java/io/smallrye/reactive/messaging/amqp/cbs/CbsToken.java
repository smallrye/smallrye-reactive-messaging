package io.smallrye.reactive.messaging.amqp.cbs;

import java.time.OffsetDateTime;

public interface CbsToken {

    /**
     * The token value
     */
    String token();

    /**
     * The type of the token
     * "servicebus.windows.net:sastoken" or "jwt"
     */
    String type();

    /**
     * The time at which the token expires.
     */
    OffsetDateTime expiresAt();

    /**
     * The time at which the token should be refreshed.
     * Defaults to expiration time — implementations should override to refresh earlier.
     */
    default OffsetDateTime refreshAt() {
        return expiresAt();
    }

}
