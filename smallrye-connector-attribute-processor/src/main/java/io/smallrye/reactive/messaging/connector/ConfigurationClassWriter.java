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
        generate(incoming, common, packageName, incomingConfigClassName, incomingConfigSimpleClassName,
                parentConfigSimpleClassName, "incoming", connector.value());

        ClassWriter.log(
                "Generating outgoing configuration for connector `%s`: %s", connector.value(), outgoingConfigClassName);
        generate(outgoing, common, packageName, outgoingConfigClassName, outgoingConfigSimpleClassName,
                parentConfigSimpleClassName, "outgoing", connector.value());
    }

    private void generate(List<ConnectorAttribute> attributes, List<ConnectorAttribute> commonAttributes,
            String packageName, String configClassName, String configSimpleClassName,
            String parentConfigSimpleClassName, String direction, String connector) throws IOException {
        JavaFileObject file = environment.getFiler().createSourceFile(configClassName);
        file.delete();
        try (PrintWriter out = new PrintWriter(file.openWriter())) {
            String iface = "incoming".equals(direction)
                    ? IncomingConnectorConfiguration.class.getName()
                    : OutgoingConnectorConfiguration.class.getName();
            String ifaceSimpleName = "incoming".equals(direction)
                    ? IncomingConnectorConfiguration.class.getSimpleName()
                    : OutgoingConnectorConfiguration.class.getSimpleName();
            writePackageDeclaration(packageName, out);
            writeImportStatements(out);
            out.println("import java.util.HashMap;");
            out.println("import java.util.Objects;");
            out.println("import java.util.Map;");
            out.println("import " + MapBasedConfig.class.getName() + ";");
            out.println("import " + iface + ";");
            out.println("import org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory;");
            writeClassDeclaration(configSimpleClassName, direction, connector, out, parentConfigSimpleClassName,
                    ifaceSimpleName);
            writeConstructor(configSimpleClassName, out);
            attributes.forEach(ca -> generateGetterForAttribute(ca, connector, out));
            writeValidateMethod(attributes, out);
            writeBuilder(configSimpleClassName, direction, connector, attributes, commonAttributes, out);
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
        out.println("  " + configSimpleClassName + "(Builder builder) {");
        out.println("    super(new MapBasedConfig(builder.values));");
        out.println("  }");
        out.println();
    }

    private void writeClassDeclaration(String configSimpleClassName, String direction, String connector,
            PrintWriter out, String parentClass, String ifaceSimpleName) {
        out.println();
        out.println("/**");
        out.println(" * Extract the " + direction + " configuration for the {@code " + connector + "} connector.");
        out.println("*/");
        out.print(String.format("public class %s extends %s implements %s {", configSimpleClassName, parentClass,
                ifaceSimpleName));
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
        out.println();
    }

    private static void writeBuilder(String configSimpleClassName, String direction, String connector,
            List<ConnectorAttribute> directionAttributes, List<ConnectorAttribute> commonAttributes,
            PrintWriter out) {
        out.println("  /**");
        out.println("   * Creates a new builder for {@code " + configSimpleClassName + "}.");
        out.println("   */");
        out.println("  public static Builder builder(String channel) {");
        out.println("    return new Builder(channel);");
        out.println("  }");
        out.println();
        out.println("  /**");
        out.println("   * Builder for {@code " + configSimpleClassName + "}.");
        out.println("   */");
        out.println("  public static class Builder {");
        out.println("    private final Map<String, String> values = new HashMap<>();");
        out.println();
        out.println("    Builder(String channel) {");
        out.println("      Objects.requireNonNull(channel, \"channel name must not be null\");");
        out.println("      values.put(ConnectorFactory.CONNECTOR_ATTRIBUTE, \"" + connector + "\");");
        out.println("      values.put(ConnectorFactory.CHANNEL_NAME_ATTRIBUTE, channel);");
        out.println("    }");
        out.println();

        // generic property setter
        out.println("    public Builder property(String key, String value) {");
        out.println("      values.put(key, value);");
        out.println("      return this;");
        out.println("    }");
        out.println();

        // setters for common attributes
        for (ConnectorAttribute ca : commonAttributes) {
            writeBuilderSetter(ca, out);
        }
        // setters for direction-specific attributes
        for (ConnectorAttribute ca : directionAttributes) {
            writeBuilderSetter(ca, out);
        }

        // build method
        out.println("    public " + configSimpleClassName + " build() {");
        out.println("      return new " + configSimpleClassName + "(this);");
        out.println("    }");

        out.println("  }");
        out.println();
    }

    private static void writeBuilderSetter(ConnectorAttribute ca, PrintWriter out) {
        String setterName = getSetterName(ca);
        String targetType = getTargetType(ca);
        boolean isString = ca.type().equalsIgnoreCase("string");
        out.println("    public Builder " + setterName + "(" + targetType + " value) {");
        out.println("      values.put(\"" + ca.name() + "\", " + (isString ? "value" : "String.valueOf(value)") + ");");
        out.println("      return this;");
        out.println("    }");
        out.println();
    }

    private static String getSetterName(ConnectorAttribute attribute) {
        String methodName = getMethodName(attribute);
        // Remove "get" prefix and lower-case the first letter
        if (methodName.startsWith("get") && methodName.length() > 3) {
            return Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4);
        }
        return methodName;
    }
}
