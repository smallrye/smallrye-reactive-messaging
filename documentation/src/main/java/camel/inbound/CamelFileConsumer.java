package camel.inbound;

import java.io.File;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.camel.component.file.GenericFile;
import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class CamelFileConsumer {

    @Incoming("files")
    public void consume(GenericFile<File> gf) {
        File file = gf.getFile();
        // process the file

    }

}
