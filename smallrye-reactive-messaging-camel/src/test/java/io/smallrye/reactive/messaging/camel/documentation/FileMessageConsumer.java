package io.smallrye.reactive.messaging.camel.documentation;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.camel.component.file.GenericFile;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
public class FileMessageConsumer {

    private final List<String> list = new ArrayList<>();

    @Incoming("files")
    public CompletionStage<Void> consume(Message<GenericFile<File>> msg) {
        File actualFile = msg.getPayload().getFile();
        list.add(actualFile.getName());
        return msg.ack();
    }

    public List<String> list() {
        return list;
    }

}
