package camel.inbound;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.camel.Exchange;
import org.apache.camel.component.file.GenericFile;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.camel.IncomingExchangeMetadata;

@ApplicationScoped
public class IncomingCamelMetadataExample {

    @Incoming("files")
    public CompletionStage<Void> consume(Message<GenericFile<File>> msg) {
        Optional<IncomingExchangeMetadata> metadata = msg.getMetadata(IncomingExchangeMetadata.class);
        if (metadata.isPresent()) {
            // Retrieve the camel exchange:
            Exchange exchange = metadata.get().getExchange();
        }
        return msg.ack();
    }

}
