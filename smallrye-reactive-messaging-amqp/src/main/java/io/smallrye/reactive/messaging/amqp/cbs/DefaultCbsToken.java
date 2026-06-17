package io.smallrye.reactive.messaging.amqp.cbs;

import java.time.OffsetDateTime;

public record DefaultCbsToken(String token, String type, OffsetDateTime expiresAt,
        OffsetDateTime refreshAt) implements CbsToken {

    public DefaultCbsToken(String token, String type, OffsetDateTime expiresAt) {
        this(token, type, expiresAt, expiresAt);
    }

    public DefaultCbsToken(String token, String type, long validityInSeconds) {
        this(token, type, OffsetDateTime.now().plusSeconds(validityInSeconds));
    }

}
