package camel.inbound;

import java.io.File;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.camel.component.file.GenericFile;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
public class CamelFileMessageConsumer {

    @Incoming("files")
    public CompletionStage<Void> consume(Message<GenericFile<File>> msg) {
        File file = msg.getPayload().getFile();
        // process the file

        return msg.ack();
    }

}
