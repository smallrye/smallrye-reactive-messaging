package connectors;

import java.util.concurrent.Flow;

import org.eclipse.microprofile.reactive.messaging.Message;

import connectors.api.BrokerClient;
import connectors.api.SendMessage;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import io.vertx.mutiny.core.Vertx;

public class MyOutgoingChannel {
    private final String channel;
    private Flow.Subscriber<? extends Message<?>> subscriber;
    private final BrokerClient client;
    private final String topic;

    public MyOutgoingChannel(Vertx vertx, MyConnectorOutgoingConfiguration oc, BrokerClient client) {
        this.channel = oc.getChannel();
        this.client = client;
        this.topic = oc.getTopic().orElse(oc.getChannel());
        this.subscriber = MultiUtils.via(multi -> multi.call(m -> publishMessage(this.client, m)));
    }

    private Uni<Void> publishMessage(BrokerClient client, Message<?> m) {
        // construct the outgoing message
        SendMessage sendMessage = new SendMessage();
        Object payload = m.getPayload();
        sendMessage.setPayload(payload);
        sendMessage.setTopic(topic);
        m.getMetadata(MyOutgoingMetadata.class).ifPresent(out -> {
            sendMessage.setTopic(out.getTopic());
            sendMessage.setKey(out.getKey());
            //...
        });
        return Uni.createFrom().completionStage(() -> client.send(sendMessage))
                .onItem().transformToUni(receipt -> Uni.createFrom().completionStage(m.ack()))
                .onFailure().recoverWithUni(t -> Uni.createFrom().completionStage(m.nack(t)));
    }

    public Flow.Subscriber<? extends Message<?>> getSubscriber() {
        return this.subscriber;
    }

    public String getChannel() {
        return this.channel;
    }
}
