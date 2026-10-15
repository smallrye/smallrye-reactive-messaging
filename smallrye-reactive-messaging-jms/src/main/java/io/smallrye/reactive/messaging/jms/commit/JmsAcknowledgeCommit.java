package io.smallrye.reactive.messaging.jms.commit;

import static io.smallrye.reactive.messaging.jms.i18n.JmsExceptions.ex;

import java.util.concurrent.Executor;

import jakarta.jms.JMSException;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.jms.IncomingJmsMessage;

public class JmsAcknowledgeCommit implements JmsCommitHandler {

    private final Executor executor;

    public JmsAcknowledgeCommit(Executor executor) {
        this.executor = executor;
    }

    @Override
    public <T> Uni<Void> handle(IncomingJmsMessage<T> message, Metadata metadata) {
        return Uni.createFrom().voidItem()
                .invoke(() -> {
                    try {
                        message.unwrap(jakarta.jms.Message.class).acknowledge();
                    } catch (JMSException e) {
                        throw ex.jmsTransactionFailure("acknowledge", e);
                    }
                })
                .runSubscriptionOn(executor)
                .emitOn(message::runOnMessageContext);
    }
}
