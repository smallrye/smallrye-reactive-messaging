package io.smallrye.reactive.messaging.connector;

import java.io.PrintWriter;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.microprofile.config.Config;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;

public class ClassWriter {

    protected static final Logger LOGGER = Logger
            .getLogger("SmallRye Reactive Messaging - Connector Attribute Processor");

    private ClassWriter() {
        // Avoid direct instantiation
    }

    static String getPackage(String className) {
        int indexOfLastDot = className.lastIndexOf('.');
        if (indexOfLastDot > 0) {
            return className.substring(0, indexOfLastDot);
        } else {
            throw new IllegalArgumentException(
                    "Invalid class name, connector classes cannot be in the default package");
        }
    }

    static String getConfigClassName(String className, String suffix) {
        return className + suffix;
    }

    static String getSimpleClassName(String className) {
        int indexOfLastDot = className.lastIndexOf('.');
        if (indexOfLastDot > 0) {
            return className.substring(indexOfLastDot + 1);
        } else {
            throw new IllegalArgumentException(
                    "Invalid class name, connector classes cannot be in the default package");
        }
    }

    static void log(String message, Object... params) {
        LOGGER.log(Level.INFO, () -> String.format(message, params));
    }

    static void writePackageDeclaration(String packageName, PrintWriter out) {
        if (packageName != null) {
            out.print("package ");
            out.print(packageName);
            out.println(";");
            out.println();
        }
    }

    static void writeImportStatements(PrintWriter out) {
        out.println("import " + Optional.class.getName() + ";");
        out.println("import " + Config.class.getName() + ";");
    }

    static String getTargetDotClassName(ConnectorAttribute attribute) {
        String lowerCase = attribute.type().toLowerCase();
        switch (lowerCase) {
            case "boolean":
                return "Boolean.class";
            case "int":
                return "Integer.class";
            case "string":
                return "String.class";
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

    static String getTargetType(ConnectorAttribute attribute) {
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

    protected static boolean hasAlias(ConnectorAttribute attribute) {
        return !attribute.alias().equals(ConnectorAttribute.NO_VALUE);
    }

    protected static boolean hasDefaultValue(ConnectorAttribute attribute) {
        return !attribute.defaultValue().equals(ConnectorAttribute.NO_VALUE);
    }

    protected static String getGetterSignatureLine(ConnectorAttribute attribute) {
        if (attribute.mandatory() || hasDefaultValue(attribute)) {
            return String.format("  public %s %s() {", getTargetType(attribute), getMethodName(attribute));
        } else {
            return String.format("  public Optional<%s> %s() {", getTargetType(attribute), getMethodName(attribute));
        }
    }

    protected static String getGetterBody(ConnectorAttribute attribute, String connector) {
        String name = attribute.name();
        String targetType = ClassWriter.getTargetType(attribute);
        String targetTypeDotClass = ClassWriter.getTargetDotClassName(attribute);
        // Mandatory attribute
        boolean hasAlias = hasAlias(attribute);
        String alias = attribute.alias();
        if (attribute.mandatory()) {
            if (hasAlias) {
                // With alias: get the attribute -> (get the alias -> fail if not present)
                return String.format("    return config.getOptionalValue(\"%s\", %s)\n"
                        + "     .orElseGet(() -> getFromAlias(\"%s\", %s)"
                        + "        .orElseThrow(() -> new IllegalArgumentException(\"The attribute `%s` (alias `%s`) on connector '%s' (channel: \" + getChannel() + \") must be set\"))"
                        + "     );",
                        name, targetTypeDotClass, alias, targetTypeDotClass, name, alias, connector);
            } else {
                // Without alias get the attribute -> fail if not present
                return String.format("    return config.getOptionalValue(\"%s\", %s)\n"
                        + "        .orElseThrow(() -> new IllegalArgumentException(\"The attribute `%s` on connector '%s' (channel: \" + getChannel() + \") must be set\"));",
                        name, targetTypeDotClass, name, connector);
            }
        } else if (hasDefaultValue(attribute)) {
            if (hasAlias) {
                // With alias and default value: get the attribute -> (get the alias -> get the default value)
                return String.format("    return config.getOptionalValue(\"%s\", %s)\n"
                        + "     .orElseGet(() -> getFromAliasWithDefaultValue(\"%s\", %s, %s));",
                        name, targetTypeDotClass, alias, targetTypeDotClass, getDefaultValue(attribute));
            } else {
                // No alias -> get the attribute -> get the default value
                return String.format("    return config.getOptionalValue(\"%s\", %s)\n"
                        + "     .orElse(%s);",
                        name, targetTypeDotClass, getDefaultValue(attribute));
            }
        } else {
            // no default value.
            if (hasAlias) {
                // With alias, without default value: get the attribute -> get the alias (must return an optional)
                return String.format(
                        "    Optional<%s> maybe = config.getOptionalValue(\"%s\", %s);\n"
                                + "    if (maybe.isPresent()) { return maybe; }\n"
                                + "    return getFromAlias(\"%s\", %s);",
                        targetType, name, targetTypeDotClass, alias, targetTypeDotClass);
            } else {
                // No alias and no default value: get the attribute
                return String.format("    return config.getOptionalValue(\"%s\", %s);", name, targetTypeDotClass);
            }
        }
    }

    protected static String getMethodName(ConnectorAttribute attribute) {
        String name = attribute.name();
        return "get" + toTitleCase(name);
    }

    private static String toTitleCase(String input) {
        if (input == null || input.trim().isEmpty()) {
            throw new IllegalArgumentException("Invalid attribute name");
        }

        String sanitized = input
                .replace("-", " ")
                .replace("_", " ")
                .replace(".", " ")
                .trim();

        StringBuilder titleCase = new StringBuilder(sanitized.length());
        boolean nextTitleCase = true;
        for (char c : sanitized.toCharArray()) {
            if (Character.isSpaceChar(c)) {
                nextTitleCase = true;
            } else if (nextTitleCase) {
                c = Character.toTitleCase(c);
                nextTitleCase = false;
                titleCase.append(c);
            } else {
                titleCase.append(c);
            }
        }
        return titleCase.toString();
    }

    static String getDefaultValue(ConnectorAttribute attribute) {
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

    static void generateGetterForAttribute(ConnectorAttribute ca, String connector, PrintWriter out) {
        out.println("  /**");
        out.println("  * Gets the " + ca.name() + " value from the configuration.");
        out.println("  * Attribute Name: " + ca.name());
        out.println("  * Description: " + ca.description());
        if (hasAlias(ca)) {
            out.println("  * MicroProfile Config Alias: " + ca.alias());
        }

        if (ca.mandatory()) {
            out.println("  * Mandatory: yes");
        } else if (hasDefaultValue(ca)) {
            out.println("  * Default Value: " + ca.defaultValue());
        }

        out.println("  * @return the " + ca.name());
        if (ca.deprecated()) {
            out.println("@Deprecated");
        }
        out.println("  */");
        out.println(ClassWriter.getGetterSignatureLine(ca));
        out.println(ClassWriter.getGetterBody(ca, connector));
        out.println("  }");
        out.println();
    }
}
