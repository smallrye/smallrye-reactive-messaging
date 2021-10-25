package io.smallrye.reactive.messaging.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.processing.ProcessingEnvironment;

import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;

public class ConfigurationDocWriterTest {

    @Test
    public void test() throws IOException {
        ProcessingEnvironment pe = mock(ProcessingEnvironment.class);
        ConfigurationDocWriter writer = new ConfigurationDocWriter(pe);

        List<ConnectorAttribute> attributes = new ArrayList<>();
        attributes.add(new ConnectorAttributeLiteral("a", "desc-a", "string", true, ConnectorAttribute.Direction.INCOMING));
        attributes.add(new ConnectorAttributeLiteral("b", "desc-b", "int", false, ConnectorAttribute.Direction.INCOMING));
        attributes.add(new ConnectorAttributeLiteral("c", "desc-c", "boolean", true, ConnectorAttribute.Direction.INCOMING)
                .setAlias("alias-c"));
        attributes.add(new ConnectorAttributeLiteral("d", "desc-d", "string", false, ConnectorAttribute.Direction.INCOMING)
                .setDefaultValue("d"));
        attributes.add(new ConnectorAttributeLiteral("e", "desc-e", "string", false, ConnectorAttribute.Direction.INCOMING)
                .setDefaultValue("e").setAlias("alias-e"));
        attributes.add(new ConnectorAttributeLiteral("f", "desc-f", "string", true, ConnectorAttribute.Direction.INCOMING)
                .setAlias("alias-f"));

        Connector connector = ConnectorLiteral.of("my-connector");

        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        try (PrintWriter pw = new PrintWriter(boas)) {
            writer.generateDocumentation(connector, "Incoming", attributes, Collections.emptyList(), pw);
            pw.flush();
        }
        String content = boas.toString("UTF-8");
        assertThat(content).isNotEmpty();

        boas = new ByteArrayOutputStream();
        try (PrintWriter pw = new PrintWriter(boas)) {
            writer.generateDocumentation(connector, "Outgoing", attributes, Collections.emptyList(), pw);
            pw.flush();
        }
        content = boas.toString("UTF-8");
        assertThat(content).isNotEmpty();
    }

}
