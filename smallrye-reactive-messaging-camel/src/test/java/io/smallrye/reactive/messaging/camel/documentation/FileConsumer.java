package io.smallrye.reactive.messaging.camel.documentation;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.camel.component.file.GenericFile;
import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class FileConsumer {

    private final List<String> list = new ArrayList<>();

    @Incoming("files")
    public void consume(GenericFile<File> file) {
        File actualFile = file.getFile();
        list.add(actualFile.getName());
    }

    public List<String> list() {
        return list;
    }

}
