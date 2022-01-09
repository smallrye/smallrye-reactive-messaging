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

public class ConfigurationMarkdownDocWriter {
    private final ProcessingEnvironment environment;

    public ConfigurationMarkdownDocWriter(ProcessingEnvironment env) {
        this.environment = env;
    }

    public void generate(Connector connector, List<ConnectorAttribute> commonAttributes,
            List<ConnectorAttribute> incomingAttributes, List<ConnectorAttribute> outgoingAttributes) throws IOException {
        generateIncomingDocumentation(connector, commonAttributes, incomingAttributes);
        generateOutgoingDocumentation(connector, commonAttributes, outgoingAttributes);

    }

    private void generateIncomingDocumentation(Connector connector, List<ConnectorAttribute> commonAttributes,
            List<ConnectorAttribute> incomingAttributes)
            throws IOException {
        FileObject resource = environment.getFiler()
                .createResource(StandardLocation.CLASS_OUTPUT, "",
                        "META-INF/connector/" + connector.value() + "-incoming.md");
        resource.delete();
        try (PrintWriter writer = new PrintWriter(resource.openWriter())) {
            generateDocumentation(commonAttributes, incomingAttributes, writer);
        }
    }

    private void generateOutgoingDocumentation(Connector connector, List<ConnectorAttribute> commonAttributes,
            List<ConnectorAttribute> outgoingAttributes)
            throws IOException {
        FileObject resource = environment.getFiler()
                .createResource(StandardLocation.CLASS_OUTPUT, "",
                        "META-INF/connector/" + connector.value() + "-outgoing.md");
        resource.delete();

        try (PrintWriter writer = new PrintWriter(resource.openWriter())) {
            generateDocumentation(commonAttributes, outgoingAttributes, writer);
        }
    }

    void generateDocumentation(List<ConnectorAttribute> commonAttributes,
            List<ConnectorAttribute> specificAttributes, PrintWriter out) {
        List<ConnectorAttribute> list = new ArrayList<>(commonAttributes);
        list.addAll(specificAttributes);
        list.sort(Comparator.comparing(ConnectorAttribute::name));

        writeTableBegin(out);
        list.forEach(att -> {
            if (!att.hiddenFromDocumentation()) {
                generateLine(att, out);
            }
        });
    }

    private void writeTableBegin(PrintWriter out) {
        out.println("|Attribute (_alias_) | Description | Type   | Mandatory | Default |");
        out.println("| :---               | :----       | :----: | :----:    | :---    |");
    }

    private void generateLine(ConnectorAttribute att, PrintWriter out) {
        String name = "*" + att.name() + "*";
        if (hasAlias(att)) {
            name += " _(" + att.alias() + ")_";
        }
        out.println(String.format("| %s | %s | %s | %s | %s |",
                name, getDescription(att).replace("\n", "").trim(), att.type(), att.mandatory(), getDefaultValueOrEmpty(att)));
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
