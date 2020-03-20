package io.smallrye.reactive.messaging.connector;

import static io.smallrye.reactive.messaging.connector.ClassWriter.hasAlias;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.FileObject;
import javax.tools.StandardLocation;

import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;

public class ConfigurationDocWriter {
    private final ProcessingEnvironment environment;

    public ConfigurationDocWriter(ProcessingEnvironment env) {
        this.environment = env;
    }

    public void generateIncomingDocumentation(Connector connector, List<ConnectorAttribute> commonAttributes,
            List<ConnectorAttribute> incomingAttributes)
            throws IOException {
        FileObject resource = environment.getFiler()
                .createResource(StandardLocation.CLASS_OUTPUT, "",
                        "META-INF/connector/" + connector.value() + "-incoming.adoc");
        resource.delete();
        try (PrintWriter out = new PrintWriter(resource.openWriter())) {
            out.println(".Incoming Attributes of the '" + connector.value() + "' connector");
            writeTableBegin(out);
            commonAttributes.forEach(att -> {
                if (!att.hiddenFromDocumentation()) {
                    generateLine(att, out);
                }
            });
            incomingAttributes.forEach(att -> {
                if (!att.hiddenFromDocumentation()) {
                    generateLine(att, out);
                }
            });
            out.println("|===");
        }
    }

    public void generateOutgoingDocumentation(Connector connector, List<ConnectorAttribute> commonAttributes,
            List<ConnectorAttribute> incomingAttributes)
            throws IOException {
        FileObject resource = environment.getFiler()
                .createResource(StandardLocation.CLASS_OUTPUT, "",
                        "META-INF/connector/" + connector.value() + "-outgoing.adoc");
        resource.delete();

        // merge and sort the attributes
        List<ConnectorAttribute> list = new ArrayList<>(commonAttributes);
        list.addAll(incomingAttributes);
        list.sort(Comparator.comparing(ConnectorAttribute::name));

        try (PrintWriter out = new PrintWriter(resource.openWriter())) {
            out.println(".Outgoing Attributes of the '" + connector.value() + "' connector");
            writeTableBegin(out);
            list.forEach(att -> {
                if (!att.hiddenFromDocumentation()) {
                    generateLine(att, out);
                }
            });
            out.println("|===");
        }
    }

    private void writeTableBegin(PrintWriter out) {
        out.println("[cols=\"25, 30, 15, 20\",options=\"header\"]");
        out.println("|===");
        out.println("|Attribute (_alias_) | Description | Mandatory | Default");
        out.println();
    }

    private void generateLine(ConnectorAttribute att, PrintWriter out) {
        String name = "*" + att.name() + "*";
        if (hasAlias(att)) {
            name += "\n\n_(" + att.alias() + ")_";
        }
        out.println(String.format("| %s | %s | %s | %s",
                name, getDescription(att) + "\n\nType: _" + att.type() + "_", att.mandatory(), getDefaultValueOrEmpty(att)));
        out.println();
    }

    private String getDescription(ConnectorAttribute att) {
        if (att.deprecated()) {
            return "_deprecated_ - " + att.description();
        }
        return att.description();
    }

    private String getDefaultValueOrEmpty(ConnectorAttribute att) {
        if (att.defaultValue().equals(ConnectorAttribute.NO_VALUE)) {
            return "";
        } else {
            return "`" + att.defaultValue() + "`";
        }
    }

    private String getAliasOrEmpty(ConnectorAttribute att) {
        if (att.alias().equals(ConnectorAttribute.NO_VALUE)) {
            return "";
        } else {
            return att.alias();
        }
    }
}
