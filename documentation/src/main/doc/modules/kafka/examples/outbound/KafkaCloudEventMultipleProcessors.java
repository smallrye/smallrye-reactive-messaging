package outbound;

import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;
import java.net.URI;

@ApplicationScoped
public class KafkaCloudEventMultipleProcessors {

    @Incoming("source")
    @Outgoing("processed")
    public Message<String> process(Message<String> in) {
        return in.addMetadata(OutgoingCloudEventMetadata.builder()
            .withId("id-" + in.getPayload())
            .withType("greeting")
            .build());
    }

    @SuppressWarnings("unchecked")
    @Incoming("processed")
    @Outgoing("cloud-events")
    public Message<String> process2(Message<String> in) {
        OutgoingCloudEventMetadata<String> metadata = in
            .getMetadata(OutgoingCloudEventMetadata.class)
            .orElseGet(() -> OutgoingCloudEventMetadata.builder().build());

        return in.addMetadata(OutgoingCloudEventMetadata.from(metadata)
            .withSource(URI.create("source://me"))
            .withSubject("test")
            .build());
    }

}
