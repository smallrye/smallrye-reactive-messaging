package inbound;

import org.apache.camel.component.file.GenericFile;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import javax.enterprise.context.ApplicationScoped;
import java.io.File;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class CamelFileMessageConsumer {

    @Incoming("files")
    public CompletionStage<Void> consume(Message<GenericFile<File>> msg) {
        File file = msg.getPayload().getFile();
        // process the file

        return msg.ack();
    }


}
