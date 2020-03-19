package io.smallrye.reactive.messaging.connector;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Optional;

import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.JavaFileObject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;

public class ConfigurationClassWriter {

    private final ProcessingEnvironment environment;

    public ConfigurationClassWriter(ProcessingEnvironment environment) {
        this.environment = environment;
    }

    public void generateIncomingConfigurationClass(Connector connector, String className,
            List<ConnectorAttribute> incomingAttributes) throws IOException {
        String packageName = null;
        int lastDot = className.lastIndexOf('.');
        if (lastDot > 0) {
            packageName = className.substring(0, lastDot);
        }
        String configClassName = className + "IncomingConfiguration";
        String configSimpleClassName = configClassName.substring(lastDot + 1);

        System.out.println(
                "Generating incoming configuration for connector: " + connector.value() + ": " + configSimpleClassName);

        generate(incomingAttributes, packageName, configClassName, configSimpleClassName,
                "incoming", connector.value());
    }

    public void generateOutgoingConfigurationClass(Connector connector,
            String className, List<ConnectorAttribute> attributes) throws IOException {
        String packageName = null;
        int lastDot = className.lastIndexOf('.');
        if (lastDot > 0) {
            packageName = className.substring(0, lastDot);
        }
        String configClassName = className + "OutgoingConfiguration";
        String configSimpleClassName = configClassName.substring(lastDot + 1);

        System.out.println(
                "Generating outgoing configuration for connector: " + connector.value() + ": " + configSimpleClassName);

        generate(attributes, packageName, configClassName, configSimpleClassName, "outgoing", connector.value());
    }

    static void writeConstructorAndConfigAccessor(String configSimpleClassName, PrintWriter out) {
        out.println("  private final Config config;");
        out.println();
        out.println("  /**");
        out.println("   * Creates a new " + configSimpleClassName + ".");
        out.println("   * The passed configuration is validated automatically.");
        out.println("   */");
        out.println("  public " + configSimpleClassName + "(Config config) {");
        out.println("    this.config = config;");
        out.println("    validate();");
        out.println("  }");
        out.println();
        out.println("  /**");
        out.println("   * @return the connector configuration");
        out.println("   */");
        out.println("  public Config config() {");
        out.println("    return this.config;");
        out.println("  }");
        out.println();
    }

    static void writeChannelNameGetter(PrintWriter out) {
        out.println("  /**");
        out.println("   * @return the channel name");
        out.println("   */");
        out.println("  public String getChannel() {");
        out.println("    return config.getValue(ConnectorFactory.CHANNEL_NAME_ATTRIBUTE, String.class);");
        out.println("  }");
        out.println();
    }

    static void writeClassDeclaration(String configSimpleClassName, String direction, String connector,
            PrintWriter out) {
        out.println();
        out.println("/**");
        out.println(" * Extract the " + direction + " configuration for the {@code " + connector + "} connector.");
        out.println("*/");
        out.print("public class ");
        out.print(configSimpleClassName);
        out.println(" {");
        out.println();
    }

    static void writePackageDeclaration(String packageName, PrintWriter out) {
        if (packageName != null) {
            out.print("package ");
            out.print(packageName);
            out.println(";");
            out.println();
        }
    }

    void generate(List<ConnectorAttribute> attributes, String packageName,
            String configClassName,
            String configSimpleClassName, String direction, String connector) throws IOException {
        JavaFileObject file = environment.getFiler().createSourceFile(configClassName);
        try (PrintWriter out = new PrintWriter(file.openWriter())) {
            writePackageDeclaration(packageName, out);
            writeImportStatements(out);
            writeClassDeclaration(configSimpleClassName, direction, connector, out);
            writeConstructorAndConfigAccessor(configSimpleClassName, out);
            writeChannelNameGetter(out);

            attributes.forEach(ca -> generateGetterForAttribute(out, ca));

            writeValidateMethod(attributes, connector, out);

            out.println("}"); // End of class.
        }
    }

    static void writeImportStatements(PrintWriter out) {
        out.println("import " + Optional.class.getName() + ";");
        out.println("import " + Config.class.getName() + ";");
        out.println("import " + ConnectorFactory.class.getName() + ";");
    }

    static void generateGetterForAttribute(PrintWriter out, ConnectorAttribute ca) {
        out.println("  /**");
        out.println("  * Gets the " + ca.name() + " value from the configuration.");
        out.println("  * Attribute Name: " + ca.name());
        out.println("  * Description: " + ca.description());
        if (ca.mandatory()) {
            out.println("  * Mandatory: yes");
        } else if (hasDefaultValue(ca)) {
            out.println("  * Default Value: " + ca.defaultValue());
        }
        out.println("  * @return the " + ca.name());
        out.println("  */");
        if (ca.mandatory()) {
            out.println("  public " + getTargetClass(ca) + " " + sanitize(ca.name()) + "() {");
        } else if (!hasDefaultValue(ca)) {
            out.println("  public Optional<" + getTargetClass(ca) + "> " + sanitize(ca.name()) + "() {");
        } else {
            out.println("  public " + getTargetClass(ca) + " " + sanitize(ca.name()) + "() {");
        }
        out.println("    " + getFromConfig(ca));
        out.println("  }");
        out.println();
    }

    static String sanitize(String attributeName) {
        return attributeName.replace("-", "_").replace(" ", "_");
    }

    static boolean hasDefaultValue(ConnectorAttribute attribute) {
        return !attribute.defaultValue().equals(ConnectorAttribute.NO_VALUE);
    }

    static String getTargetClassName(ConnectorAttribute attribute) {
        String lowerCase = attribute.type().toLowerCase();
        switch (lowerCase) {
            case "boolean":
                return "Boolean.class";
            case "int":
                return "Integer.class";
            case "string":
                return "java.lang.String.class";
            case "double":
                return "Double.class";
            case "float":
                return "Float.class";
            case "short":
                return "Short.class";
            case "long":
                return "Long.class";
            case "byte":
                return "Byte.class";
            default:
                return attribute.type() + ".class";
        }
    }

    static String getTargetClass(ConnectorAttribute attribute) {
        String lowerCase = attribute.type().toLowerCase();
        switch (lowerCase) {
            case "boolean":
                return "Boolean";
            case "int":
                return "Integer";
            case "string":
                return "String";
            case "double":
                return "Double";
            case "float":
                return "Float";
            case "short":
                return "Short";
            case "long":
                return "Long";
            case "byte":
                return "Byte";
            default:
                return attribute.type();
        }
    }

    static String convert(ConnectorAttribute attribute) {
        String defaultValue = attribute.defaultValue();
        String type = attribute.type();
        if (!hasDefaultValue(attribute)) {
            switch (type.toLowerCase()) {
                case "boolean":
                    return "false";
                case "int":
                    return "0";
                case "double":
                    return "0.0";
                case "float":
                    return "0.0f";
                case "short":
                    return "(short) 0";
                case "long":
                    return "0L";
                case "byte":
                    return "(byte) 0";
                default:
                    return "null";
            }
        }

        switch (type.toLowerCase()) {
            case "boolean":
                return "Boolean.valueOf(\"" + defaultValue + "\")";
            case "int":
                return "Integer.valueOf(\"" + defaultValue + "\")";
            case "string":
                return "\"" + defaultValue + "\"";
            case "double":
                return "Double.valueOf(\"" + defaultValue + "\")";
            case "float":
                return "Float.valueOf(\"" + defaultValue + "\")";
            case "short":
                return "Short.valueOf(\"" + defaultValue + "\")";
            case "long":
                return "Long.valueOf(\"" + defaultValue + "\")";
            case "byte":
                return "Byte.valueOf(\"" + defaultValue + "\")";
            default:
                return "new " + type + "(\"" + defaultValue + "\")";
        }
    }

    private static String getFromConfig(ConnectorAttribute attribute) {
        String targetClassName = getTargetClassName(attribute);
        if (attribute.mandatory()) {
            return String.format("    return config.getValue(\"%s\", %s);", attribute.name(), targetClassName);
        } else if (!hasDefaultValue(attribute)) {
            return String.format("    return config.getOptionalValue(\"%s\", %s);", attribute.name(), targetClassName);
        } else {
            return String.format("    return config.getOptionalValue(\"%s\", %s)"
                    + ".orElse(%s);", attribute.name(), targetClassName, convert(attribute));
        }
    }

    private static void writeValidateMethod(List<ConnectorAttribute> attributes, String connector, PrintWriter out) {
        out.println("  public void validate() {");
        attributes.forEach(ca -> {
            if (ca.mandatory()) {
                out.println(String.format("    config.getOptionalValue(\"%s\", %s)"
                        + ".orElseThrow(() -> new IllegalArgumentException(\"The attribute `%s` on connector '%s' (channel: \" + getChannel() + \") must be set\"));",
                        ca.name(), getTargetClassName(ca), ca.name(), connector));
            }
        });
        out.println("  }");
    }
}
