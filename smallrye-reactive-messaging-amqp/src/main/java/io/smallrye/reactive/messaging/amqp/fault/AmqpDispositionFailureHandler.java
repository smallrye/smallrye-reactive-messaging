package io.smallrye.reactive.messaging.amqp.fault;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;

import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.amqp.AmqpAcknowledgement;
import io.smallrye.reactive.messaging.amqp.AmqpConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.amqp.AmqpMessage;
import io.smallrye.reactive.messaging.amqp.ConnectionHolder;
import io.vertx.mutiny.core.Context;

/**
 * Consolidated AMQP failure handler that resolves the disposition to apply
 * from (in priority order):
 * <ol>
 *   <li>{@link AmqpAcknowledgement} set by the processing code on the message</li>
 *   <li>The channel-level default disposition from configuration</li>
 * </ol>
 */
public class AmqpDispositionFailureHandler implements AmqpFailureHandler {

    @ApplicationScoped
    @Identifier(Strategy.FAIL)
    public static class FailFactory implements AmqpFailureHandler.Factory {
        @Override
        public AmqpFailureHandler create(AmqpConnectorIncomingConfiguration config,
                BiConsumer<String, Throwable> reportFailure) {
            return new AmqpDispositionFailureHandler(config.getChannel(),
                    io.vertx.mutiny.amqp.AmqpMessage::rejected, true, reportFailure);
        }
    }

    @ApplicationScoped
    @Identifier(Strategy.ACCEPT)
    public static class AcceptFactory implements AmqpFailureHandler.Factory {
        @Override
        public AmqpFailureHandler create(AmqpConnectorIncomingConfiguration config,
                BiConsumer<String, Throwable> reportFailure) {
            return new AmqpDispositionFailureHandler(config.getChannel(),
                    io.vertx.mutiny.amqp.AmqpMessage::accepted, false, reportFailure);
        }
    }

    @ApplicationScoped
    @Identifier(Strategy.RELEASE)
    public static class ReleaseFactory implements AmqpFailureHandler.Factory {
        @Override
        public AmqpFailureHandler create(AmqpConnectorIncomingConfiguration config,
                BiConsumer<String, Throwable> reportFailure) {
            return new AmqpDispositionFailureHandler(config.getChannel(),
                    io.vertx.mutiny.amqp.AmqpMessage::released, false, reportFailure);
        }
    }

    @ApplicationScoped
    @Identifier(Strategy.REJECT)
    public static class RejectFactory implements AmqpFailureHandler.Factory {
        @Override
        public AmqpFailureHandler create(AmqpConnectorIncomingConfiguration config,
                BiConsumer<String, Throwable> reportFailure) {
            return new AmqpDispositionFailureHandler(config.getChannel(),
                    io.vertx.mutiny.amqp.AmqpMessage::rejected, false, reportFailure);
        }
    }

    @ApplicationScoped
    @Identifier(Strategy.MODIFIED_FAILED)
    public static class ModifiedFailedFactory implements AmqpFailureHandler.Factory {
        @Override
        public AmqpFailureHandler create(AmqpConnectorIncomingConfiguration config,
                BiConsumer<String, Throwable> reportFailure) {
            return new AmqpDispositionFailureHandler(config.getChannel(),
                    m -> m.modified(true, false), false, reportFailure);
        }
    }

    @ApplicationScoped
    @Identifier(Strategy.MODIFIED_FAILED_UNDELIVERABLE_HERE)
    public static class ModifiedFailedUndeliverableHereFactory implements AmqpFailureHandler.Factory {
        @Override
        public AmqpFailureHandler create(AmqpConnectorIncomingConfiguration config,
                BiConsumer<String, Throwable> reportFailure) {
            return new AmqpDispositionFailureHandler(config.getChannel(),
                    m -> m.modified(true, true), false, reportFailure);
        }
    }

    private final String channel;
    private final Consumer<io.vertx.mutiny.amqp.AmqpMessage> defaultDisposition;
    private final boolean fatal;
    private final BiConsumer<String, Throwable> reportFailure;

    public AmqpDispositionFailureHandler(String channel,
            Consumer<io.vertx.mutiny.amqp.AmqpMessage> defaultDisposition,
            boolean fatal,
            BiConsumer<String, Throwable> reportFailure) {
        this.channel = channel;
        this.defaultDisposition = defaultDisposition;
        this.fatal = fatal;
        this.reportFailure = reportFailure;
    }

    @Override
    public <V> CompletionStage<Void> handle(AmqpMessage<V> msg, Context context, Throwable reason, Metadata metadata) {
        AmqpAcknowledgement ack = msg.getMetadata(AmqpAcknowledgement.class).orElse(null);
        Consumer<io.vertx.mutiny.amqp.AmqpMessage> disposition = resolveDisposition(ack);
        boolean isFatal = resolveFatal(ack);

        if (isFatal) {
            log.nackedFailMessage(channel);
            reportFailure.accept(channel, reason);
            return ConnectionHolder.runOnContextAndReportFailure(context, msg, reason, disposition);
        } else {
            log.nackedIgnoreMessage(channel);
            log.fullIgnoredFailure(reason);
            return ConnectionHolder.runOnContext(context, msg, disposition);
        }
    }

    private Consumer<io.vertx.mutiny.amqp.AmqpMessage> resolveDisposition(AmqpAcknowledgement ack) {
        if (ack != null && ack.getDisposition() != null) {
            return ack.getDisposition();
        }
        return defaultDisposition;
    }

    private boolean resolveFatal(AmqpAcknowledgement ack) {
        if (ack != null && ack.getDisposition() != null) {
            return ack.isFatal();
        }
        return fatal;
    }
}
