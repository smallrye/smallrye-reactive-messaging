package io.smallrye.reactive.messaging.connector;

import static io.smallrye.reactive.messaging.connector.ClassWriter.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.JavaFileObject;

import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;

public class ConfigurationClassWriter {

    private final ProcessingEnvironment environment;

    public ConfigurationClassWriter(ProcessingEnvironment environment) {
        this.environment = environment;
    }

    public void generateAllClasses(Connector connector, String className, List<ConnectorAttribute> common,
            List<ConnectorAttribute> incoming, List<ConnectorAttribute> outgoing)
            throws IOException {
        CommonConfigurationClassWriter.generate(environment, connector, className, common);

        String packageName = getPackage(className);
        String incomingConfigClassName = getConfigClassName(className, "IncomingConfiguration");
        String outgoingConfigClassName = getConfigClassName(className, "OutgoingConfiguration");
        String parentClassName = getConfigClassName(className, "CommonConfiguration");
        String incomingConfigSimpleClassName = getSimpleClassName(incomingConfigClassName);
        String outgoingConfigSimpleClassName = getSimpleClassName(outgoingConfigClassName);
        String parentConfigSimpleClassName = getSimpleClassName(parentClassName);

        ClassWriter.log(
                "Generating incoming configuration for connector `%s`: %s", connector.value(), incomingConfigClassName);
        generate(incoming, packageName, incomingConfigClassName, incomingConfigSimpleClassName, parentConfigSimpleClassName,
                "incoming", connector.value());

        ClassWriter.log(
                "Generating outgoing configuration for connector `%s`: %s", connector.value(), outgoingConfigClassName);
        generate(outgoing, packageName, outgoingConfigClassName, outgoingConfigSimpleClassName, parentConfigSimpleClassName,
                "outgoing", connector.value());
    }

    private void generate(List<ConnectorAttribute> attributes, String packageName,
            String configClassName, String configSimpleClassName, String parentConfigSimpleClassName,
            String direction, String connector) throws IOException {
        JavaFileObject file = environment.getFiler().createSourceFile(configClassName);
        file.delete();
        try (PrintWriter out = new PrintWriter(file.openWriter())) {
            writePackageDeclaration(packageName, out);
            writeImportStatements(out);
            writeClassDeclaration(configSimpleClassName, direction, connector, out, parentConfigSimpleClassName);
            writeConstructor(configSimpleClassName, out);
            attributes.forEach(ca -> generateGetterForAttribute(ca, connector, out));
            writeValidateMethod(attributes, out);
            out.println("}"); // End of class.
        }
    }

    private void writeConstructor(String configSimpleClassName, PrintWriter out) {
        out.println();
        out.println("  /**");
        out.println("   * Creates a new " + configSimpleClassName + ".");
        out.println("   */");
        out.println("  public " + configSimpleClassName + "(Config config) {");
        out.println("    super(config);");
        out.println("    validate();");
        out.println("  }");
        out.println();
    }

    private void writeClassDeclaration(String configSimpleClassName, String direction, String connector,
            PrintWriter out, String parentClass) {
        out.println();
        out.println("/**");
        out.println(" * Extract the " + direction + " configuration for the {@code " + connector + "} connector.");
        out.println("*/");
        out.print(String.format("public class %s extends %s {", configSimpleClassName, parentClass));
        out.println();
    }

    private static void writeValidateMethod(List<ConnectorAttribute> attributes, PrintWriter out) {
        out.println("  public void validate() {");
        out.println("    super.validate();");

        attributes.forEach(ca -> {
            if (ca.mandatory()) {
                out.println(String.format("    %s();", getMethodName(ca)));
            }
        });
        out.println("  }");
    }
}
