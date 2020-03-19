package io.smallrye.reactive.messaging.connector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;

import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import com.google.auto.service.AutoService;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.annotations.ConnectorAttributes;

@SupportedAnnotationTypes("io.smallrye.reactive.messaging.annotations.ConnectorAttributes")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
@AutoService(Processor.class)
public class ConnectorAttributeProcessor extends AbstractProcessor {

    @Override
    public boolean process(Set<? extends TypeElement> annotations,
            RoundEnvironment roundEnv) {
        for (Element annotatedElement : roundEnv.getElementsAnnotatedWith(ConnectorAttributes.class)) {
            String className = annotatedElement.toString();
            Connector connector = getConnector(annotatedElement);
            ConnectorAttribute[] attributes = annotatedElement.getAnnotation(ConnectorAttributes.class).value();

            List<ConnectorAttribute> incomingAttributes = new ArrayList<>();
            List<ConnectorAttribute> outgoingAttributes = new ArrayList<>();

            for (ConnectorAttribute attribute : attributes) {
                addAttributeToList(incomingAttributes, attribute, ConnectorAttribute.Direction.INCOMING);
                addAttributeToList(outgoingAttributes, attribute, ConnectorAttribute.Direction.OUTGOING);
            }

            ConfigurationClassWriter classWriter = new ConfigurationClassWriter(processingEnv);
            ConfigurationDocWriter docWriter = new ConfigurationDocWriter(processingEnv);

            try {
                if (!incomingAttributes.isEmpty()) {
                    classWriter.generateIncomingConfigurationClass(connector, className, incomingAttributes);
                    docWriter.generateIncomingDocumentation(connector, incomingAttributes);
                }
                if (!outgoingAttributes.isEmpty()) {
                    classWriter.generateOutgoingConfigurationClass(connector, className, outgoingAttributes);
                }
                docWriter.generateOutgoingDocumentation(connector, outgoingAttributes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    private void addAttributeToList(List<ConnectorAttribute> list, ConnectorAttribute attribute,
            ConnectorAttribute.Direction direction) {
        if (attribute.direction() == direction || attribute.direction() == ConnectorAttribute.Direction.INCOMING_AND_OUTGOING) {
            list.add(attribute);
        }
    }

    private Connector getConnector(Element annotatedElement) {
        Connector connector = annotatedElement.getAnnotation(Connector.class);
        if (connector == null) {
            throw new IllegalStateException(
                    "Expecting the usage of `@ConnectorAttribute` on a class annotated with @Connector");
        }
        return connector;
    }

}
