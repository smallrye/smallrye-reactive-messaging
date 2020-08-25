package outbound;

import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;
import java.net.URI;

@ApplicationScoped
public class KafkaCloudEventProcessor {

    @Outgoing("cloud-events")
    public Message<String> toCloudEvents(Message<String> in) {
        return in.addMetadata(OutgoingCloudEventMetadata.builder()
            .withId("id-" + in.getPayload())
            .withType("greetings")
            .withSource(URI.create("http://example.com"))
            .withSubject("greeting-message")
            .build());
    }

}
