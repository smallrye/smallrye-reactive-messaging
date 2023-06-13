package pulsar.outbound;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.pulsar.OutgoingMessage;
import io.smallrye.reactive.messaging.pulsar.transactions.PulsarTransactions;

@ApplicationScoped
public class PulsarTransactionalProducer {

    @Inject
    @Channel("tx-out-example")
    PulsarTransactions<OutgoingMessage<Integer>> txProducer;

    @Inject
    @Channel("other-producer")
    PulsarTransactions<String> producer;

    @Incoming("in")
    public Uni<Void> emitInTransaction(Message<Integer> in) {
        return txProducer.withTransaction(emitter -> {
            emitter.send(OutgoingMessage.of("a", 1));
            emitter.send(OutgoingMessage.of("b", 2));
            emitter.send(OutgoingMessage.of("c", 3));
            producer.send(emitter, "4");
            producer.send(emitter, "5");
            producer.send(emitter, "6");
            return Uni.createFrom().completionStage(in::ack);
        });
    }

}
