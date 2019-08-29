package awssns;

import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.aws.sns.SnsMessage;

@ApplicationScoped
public class Receiver {

    @Incoming("my-topic")
    public CompletionStage<Void> consume(SnsMessage<String> message) {
        System.out.println("received (my-topic): " + message.getPayload());
        return message.ack();
    }

}
