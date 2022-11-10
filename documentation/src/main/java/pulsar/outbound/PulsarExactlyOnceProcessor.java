package pulsar.outbound;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingBatchMessage;
import io.smallrye.reactive.messaging.pulsar.PulsarMessage;
import io.smallrye.reactive.messaging.pulsar.transactions.PulsarTransactions;

@ApplicationScoped
public class PulsarExactlyOnceProcessor {

    @Inject
    @Channel("tx-out-example")
    PulsarTransactions<Integer> txProducer;

    @Incoming("in-channel")
    public Uni<Void> emitInTransaction(PulsarIncomingBatchMessage<Integer> batch) {
        return txProducer.withTransactionAndAck(batch, emitter -> {
            for (PulsarMessage<Integer> record : batch) {
                emitter.send(PulsarMessage.of(record.getPayload() + 1, record.getKey()));
            }
            return Uni.createFrom().voidItem();
        });
    }

}
