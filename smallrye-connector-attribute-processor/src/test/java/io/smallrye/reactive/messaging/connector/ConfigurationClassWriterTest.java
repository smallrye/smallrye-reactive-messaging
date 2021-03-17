package io.smallrye.reactive.messaging.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.processing.ProcessingEnvironment;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;

public class ConfigurationClassWriterTest {

    @Test
    public void test() throws UnsupportedEncodingException {
        ProcessingEnvironment pe = mock(ProcessingEnvironment.class);
        CommonConfigurationClassWriter writer = new CommonConfigurationClassWriter(pe);

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

        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        try (PrintWriter pw = new PrintWriter(boas)) {
            writer.write(attributes, "org.acme", "MyConfig", "my-connector", pw);
            pw.flush();
        }
        String content = boas.toString("UTF-8");
        assertThat(content).contains("package org.acme;");
    }

}
