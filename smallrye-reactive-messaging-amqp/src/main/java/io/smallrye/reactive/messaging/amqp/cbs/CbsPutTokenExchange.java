package io.smallrye.reactive.messaging.amqp.cbs;

import static io.smallrye.reactive.messaging.amqp.cbs.CbsUtils.extractCbsNode;

import java.util.Date;
import java.util.regex.Pattern;

import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.amqp.AmqpConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.amqp.reply.AmqpDirectRequestReply;
import io.smallrye.reactive.messaging.amqp.reply.AmqpRequestReplyReplyException;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.amqp.AmqpConnection;
import io.vertx.mutiny.amqp.AmqpMessage;

public class CbsPutTokenExchange implements CbsExchange {

    static final String PUT_TOKEN_TYPE = "type";
    static final String PUT_TOKEN_AUDIENCE = "name";
    static final String PUT_TOKEN_EXPIRY = "expiration";
    private static final String PUT_TOKEN_OPERATION = "operation";
    private static final String PUT_TOKEN_OPERATION_VALUE = "put-token";

    public static final String UNDEFINED_STATUS_DESCRIPTION = "";
    public static final int UNDEFINED_STATUS_CODE = 306;
    public static final String UNDEFINED_ERROR_CONDITION = "";

    private static final String STATUS_CODE = "statusCode";
    private static final String STATUS_DESCRIPTION = "statusDescription";
    private static final String ERROR_CONDITION = "errorCondition";

    private static final String LEGACY_STATUS_CODE = "status-code";
    private static final String LEGACY_STATUS_DESCRIPTION = "status-description";
    private static final String LEGACY_ERROR_CONDITION = "error-condition";

    private static final String AMQP_REQUEST_FAILED_ERROR = "status-code: %s, status-description: %s";
    private static final Pattern ENTITY_NOT_FOUND_PATTERN = Pattern.compile("The messaging entity .* could not be found");

    static final String TOKEN_AUDIENCE_FORMAT = "amqp://%s/%s";
    static final String CBS_AUDIENCE_KEY = "cbs.audience";

    private final String audience;
    private final AmqpDirectRequestReply requestReply;

    @ApplicationScoped
    @Identifier("put-token")
    public static class Factory implements CbsExchange.Factory {

        @Override
        public CbsExchange create(AmqpConnection connection, AmqpConnectorCommonConfiguration config) {
            String entity = config.getAddress().orElseGet(config::getChannel);
            String audience = config.config().getOptionalValue(CBS_AUDIENCE_KEY, String.class)
                    .orElseGet(() -> String.format(TOKEN_AUDIENCE_FORMAT, config.getHost(), entity));
            return new CbsPutTokenExchange(connection, audience);
        }
    }

    public CbsPutTokenExchange(AmqpConnection connection, String audience) {
        String cbsNode = extractCbsNode(connection);
        this.requestReply = new AmqpDirectRequestReply(connection,
                cbsNode, "cbs",
                cbsNode, cbsNode.replace("$", "") + "-client-reply-to");
        this.audience = audience;
    }

    @Override
    public Uni<CbsToken> authorize(Uni<CbsToken> authentication) {
        return authentication.onItem().call(token -> requestReply.request(AmqpMessage.create()
                .applicationProperties(JsonObject.of(
                        PUT_TOKEN_OPERATION, PUT_TOKEN_OPERATION_VALUE,
                        PUT_TOKEN_EXPIRY, Date.from(token.expiresAt().toInstant()),
                        PUT_TOKEN_TYPE, token.type(),
                        PUT_TOKEN_AUDIENCE, audience))
                .withBody(token.token())
                .build()).onItem().transformToUni(reply -> {
                    if (isSuccessful(reply)) {
                        return Uni.createFrom().voidItem();
                    } else {
                        return Uni.createFrom().failure(
                                amqpResponseCodeToException(getStatusCode(reply), getStatusDescription(reply)));
                    }
                }));
    }

    public static String getStatusDescription(AmqpMessage message) {
        JsonObject properties = message.applicationProperties();

        if (properties == null) {
            return UNDEFINED_STATUS_DESCRIPTION;
        } else if (properties.containsKey(STATUS_DESCRIPTION)) {
            return String.valueOf(properties.getValue(STATUS_DESCRIPTION));
        } else if (properties.containsKey(LEGACY_STATUS_DESCRIPTION)) {
            return String.valueOf(properties.getValue(LEGACY_STATUS_DESCRIPTION));
        } else {
            return UNDEFINED_STATUS_DESCRIPTION;
        }
    }

    public static int getStatusCode(AmqpMessage message) {
        JsonObject properties = message.applicationProperties();
        if (properties == null) {
            return UNDEFINED_STATUS_CODE;
        }

        if (properties.containsKey(STATUS_CODE)) {
            return ((Number) properties.getValue(STATUS_CODE)).intValue();
        } else if (properties.containsKey(LEGACY_STATUS_CODE)) {
            return ((Number) properties.getValue(LEGACY_STATUS_CODE)).intValue();
        } else {
            return UNDEFINED_STATUS_CODE;
        }
    }

    public static boolean isSuccessful(AmqpMessage message) {
        final int statusCode = getStatusCode(message);
        return statusCode == 200 || statusCode == 202;
    }

    public static Exception amqpResponseCodeToException(int statusCode, String statusDescription) {
        final String message = String.format(AMQP_REQUEST_FAILED_ERROR, statusCode, statusDescription);
        return switch (statusCode) {
            case 400 -> new IllegalArgumentException(message);
            case 404 -> new AmqpRequestReplyReplyException(!ENTITY_NOT_FOUND_PATTERN.matcher(message).find(), message);
            case 403, 401 -> new AmqpRequestReplyReplyException(false, message);
            default -> new AmqpRequestReplyReplyException(true, message);
        };
    }

    @Override
    public void close() {
        requestReply.close();
    }

}
