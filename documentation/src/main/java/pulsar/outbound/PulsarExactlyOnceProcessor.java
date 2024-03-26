package pulsar.outbound;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingBatchMessageMetadata;
import io.smallrye.reactive.messaging.pulsar.PulsarMessage;
import io.smallrye.reactive.messaging.pulsar.transactions.PulsarTransactions;

@ApplicationScoped
public class PulsarExactlyOnceProcessor {

    @Inject
    @Channel("tx-out-example")
    PulsarTransactions<Integer> txProducer;

    @Incoming("in-channel")
    public Uni<Void> emitInTransaction(Message<List<Integer>> batch) {
        return txProducer.withTransactionAndAck(batch, emitter -> {
            PulsarIncomingBatchMessageMetadata metadata = batch.getMetadata(PulsarIncomingBatchMessageMetadata.class).get();
            for (org.apache.pulsar.client.api.Message<Integer> message : metadata.<Integer> getMessages()) {
                emitter.send(PulsarMessage.of(message.getValue() + 1, message.getKey()));
            }
            return Uni.createFrom().voidItem();
        });
    }

}
