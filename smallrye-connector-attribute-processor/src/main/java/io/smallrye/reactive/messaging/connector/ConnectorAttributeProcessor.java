package io.smallrye.reactive.messaging.connector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;

import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import com.google.auto.service.AutoService;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.annotations.ConnectorAttributes;

@SupportedAnnotationTypes({
        "io.smallrye.reactive.messaging.annotations.ConnectorAttributes",
        "io.smallrye.reactive.messaging.annotations.ConnectorAttribute"
})
@SupportedSourceVersion(SourceVersion.RELEASE_8)
@AutoService(Processor.class)
public class ConnectorAttributeProcessor extends AbstractProcessor {

    private volatile boolean invoked;

    @Override
    public boolean process(Set<? extends TypeElement> annotations,
            RoundEnvironment roundEnv) {

        if (invoked) {
            return true;
        }
        invoked = true;

        Set<? extends Element> annotated = roundEnv.getElementsAnnotatedWith(ConnectorAttributes.class);
        Set<? extends Element> others = roundEnv.getElementsAnnotatedWith(ConnectorAttribute.class);
        Set<Element> all = new LinkedHashSet<>();
        all.addAll(annotated);
        all.addAll(others);

        for (Element annotatedElement : all) {
            String className = annotatedElement.toString();
            Connector connector = getConnector(annotatedElement);
            ConnectorAttributes annotation = annotatedElement.getAnnotation(ConnectorAttributes.class);
            ConnectorAttribute[] attributes;
            if (annotation == null) {
                attributes = new ConnectorAttribute[] { annotatedElement.getAnnotation(ConnectorAttribute.class) };
            } else {
                attributes = annotation.value();
            }

            List<ConnectorAttribute> incomingAttributes = new ArrayList<>();
            List<ConnectorAttribute> outgoingAttributes = new ArrayList<>();
            List<ConnectorAttribute> commonAttributes = new ArrayList<>();

            for (ConnectorAttribute attribute : attributes) {
                addAttributeToList(commonAttributes, attribute, ConnectorAttribute.Direction.INCOMING_AND_OUTGOING);
                addAttributeToList(incomingAttributes, attribute, ConnectorAttribute.Direction.INCOMING);
                addAttributeToList(outgoingAttributes, attribute, ConnectorAttribute.Direction.OUTGOING);
            }

            validate(commonAttributes);
            validate(incomingAttributes);
            validate(outgoingAttributes);

            ConfigurationClassWriter classWriter = new ConfigurationClassWriter(processingEnv);
            ConfigurationDocWriter asciidocWriter = new ConfigurationDocWriter(processingEnv);
            ConfigurationMarkdownDocWriter markdownWriter = new ConfigurationMarkdownDocWriter(processingEnv);

            try {
                classWriter.generateAllClasses(connector, className, commonAttributes, incomingAttributes, outgoingAttributes);
                asciidocWriter.generate(connector, commonAttributes, incomingAttributes, outgoingAttributes);
                markdownWriter.generate(connector, commonAttributes, incomingAttributes, outgoingAttributes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    private void validate(List<ConnectorAttribute> attributes) {
        attributes.forEach(ca -> {
            if (ca.mandatory() && ClassWriter.hasDefaultValue(ca)) {
                throw new IllegalArgumentException(
                        "The attribute " + ca.name() + " cannot be mandatory and have a default value");
            }
        });
    }

    private void addAttributeToList(List<ConnectorAttribute> list, ConnectorAttribute attribute,
            ConnectorAttribute.Direction direction) {
        if (attribute.direction() == direction) {
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
