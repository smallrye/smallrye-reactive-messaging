package io.smallrye.reactive.messaging.jms.commit;

import static io.smallrye.reactive.messaging.jms.i18n.JmsExceptions.ex;

import java.util.concurrent.Executor;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.jms.IncomingJmsMessage;
import io.smallrye.reactive.messaging.jms.JmsSessionContext;

public class JmsLocalTransactionCommit implements JmsCommitHandler {

    private final Executor executor;
    private final Runnable onProcessed;

    public JmsLocalTransactionCommit(Executor executor, Runnable onProcessed) {
        this.executor = executor;
        this.onProcessed = onProcessed;
    }

    @Override
    public <T> Uni<Void> handle(IncomingJmsMessage<T> message, Metadata metadata) {
        return Uni.createFrom().voidItem()
                .invoke(() -> {
                    try {
                        message.getMetadata(JmsSessionContext.class)
                                .orElseThrow(() -> new IllegalStateException("No JmsSessionContext on message"))
                                .jmsContext().commit();
                    } catch (Exception e) {
                        throw ex.jmsTransactionFailure("commit", e);
                    } finally {
                        onProcessed.run();
                    }
                })
                .runSubscriptionOn(executor)
                .emitOn(message::runOnMessageContext);
    }
}
