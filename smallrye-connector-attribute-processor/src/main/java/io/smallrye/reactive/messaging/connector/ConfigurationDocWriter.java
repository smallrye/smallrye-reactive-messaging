package io.smallrye.reactive.messaging.connector;

import java.io.IOException;
import java.io.PrintWriter;
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
            out.println("|===");
            out.println("|Attribute | Description | Type | Mandatory | Default Value | Alias");
            out.println();
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
        try (PrintWriter out = new PrintWriter(resource.openWriter())) {
            out.println(".Outgoing Attributes of the '" + connector.value() + "' connector");
            out.println("|===");
            out.println("|Attribute | Description | Type | Mandatory | Default Value | Alias");
            out.println();
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

    private void generateLine(ConnectorAttribute att, PrintWriter out) {
        out.println(String.format("| %s | %s | %s | %s | %s | %s",
                att.name(), getDescription(att), att.type(), att.mandatory(), getDefaultValueOrEmpty(att),
                getAliasOrEmpty(att)));
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
