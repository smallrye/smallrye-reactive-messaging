package io.smallrye.reactive.messaging.connector;

import static io.smallrye.reactive.messaging.connector.ClassWriter.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.JavaFileObject;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;

/**
 * Generates the parent class used by the incoming and outgoing configuration classes.
 * This class stored the connector config and provide helper methods.
 */
public class CommonConfigurationClassWriter {

    private final ProcessingEnvironment environment;

    public CommonConfigurationClassWriter(ProcessingEnvironment environment) {
        this.environment = environment;
    }

    public static void generate(ProcessingEnvironment environment, Connector connector, String className,
            List<ConnectorAttribute> attributes) throws IOException {
        CommonConfigurationClassWriter writer = new CommonConfigurationClassWriter(environment);
        String packageName = ClassWriter.getPackage(className);
        String configClassName = ClassWriter.getConfigClassName(className, "CommonConfiguration");
        String simpleClassName = ClassWriter.getSimpleClassName(configClassName);
        ClassWriter.log(
                "Generating common configuration for connector `%s`: %s", connector.value(), configClassName);
        writer.generate(attributes, packageName, configClassName, simpleClassName, connector.value());
    }

    private void generate(List<ConnectorAttribute> attributes, String packageName,
            String className, String simpleName, String connector) throws IOException {
        JavaFileObject file = environment.getFiler().createSourceFile(className);
        file.delete();
        try (PrintWriter out = new PrintWriter(file.openWriter())) {
            write(attributes, packageName, simpleName, connector, out);
        }
    }

    void write(List<ConnectorAttribute> attributes, String packageName, String simpleName, String connector,
            PrintWriter out) {
        writePackageDeclaration(packageName, out);

        writeImportStatements(out);
        out.println("import " + ConfigProvider.class.getName() + ";");
        out.println("import " + ConnectorFactory.class.getName() + ";");

        writeClassDeclaration(simpleName, connector, out);
        writeConstructorAndConfigAccessor(simpleName, out);
        attributes.forEach(ca -> generateGetterForAttribute(ca, connector, out));
        writeValidateMethod(attributes, out);

        out.println("}"); // End of class.
    }

    private void writeClassDeclaration(String simpleName, String connector, PrintWriter out) {
        out.println();
        out.println("/**");
        out.println(String.format(" * Extracts the common configuration for the {@code %s} connector.", connector));
        out.println("*/");
        out.println(String.format(" public class %s {", simpleName));
    }

    private void writeConstructorAndConfigAccessor(String simpleName, PrintWriter out) {
        // The Config object
        out.println("  protected final Config config;");
        out.println();

        // The constructor
        out.println("  /**");
        out.println("   * Creates a new " + simpleName + ".");
        out.println("   */");
        out.println(String.format("  public %s(Config config) {", simpleName));
        out.println("    this.config = config;");
        out.println("  }");
        out.println();

        // Config accessor
        out.println("  /**");
        out.println("   * @return the connector configuration");
        out.println("   */");
        out.println("  public Config config() {");
        out.println("    return this.config;");
        out.println("  }");
        out.println();

        // Get from global config accessor
        out.println("  /**");
        out.println("   * Retrieves the value stored for the given alias.");
        out.println("   * @param alias the attribute alias, must not be {@code null} or blank");
        out.println("   * @param type the targeted type");
        out.println("   * @param <T> the targeted type");
        out.println("   * @return the configuration value for the given alias, empty if not set");
        out.println("   */");
        out.println("  protected <T> Optional<T> getFromAlias(String alias, Class<T> type) {");
        out.println("    return ConfigProvider.getConfig().getOptionalValue(alias, type);");
        out.println("  }");
        out.println();

        // Get from global config accessor with default value
        out.println("  /**");
        out.println("   * Retrieves the value stored for the given alias. Returns the default value if not present.");
        out.println("   * @param alias the attribute alias, must not be {@code null} or blank");
        out.println("   * @param type the targeted type");
        out.println("   * @param defaultValue the default value");
        out.println("   * @param <T> the targeted type");
        out.println("   * @return the configuration value for the given alias, empty if not set");
        out.println("   */");
        out.println("  protected <T> T getFromAliasWithDefaultValue(String alias, Class<T> type, T defaultValue) {");
        out.println("    return getFromAlias(alias, type).orElse(defaultValue);");
        out.println("  }");
        out.println();

        // Get Channel method
        out.println("  /**");
        out.println("   * @return the channel name");
        out.println("   */");
        out.println("  public String getChannel() {");
        out.println("    return config.getValue(ConnectorFactory.CHANNEL_NAME_ATTRIBUTE, String.class);");
        out.println("  }");
        out.println();
    }

    private void writeValidateMethod(List<ConnectorAttribute> attributes, PrintWriter out) {
        out.println("  public void validate() {");
        attributes.forEach(ca -> {
            if (ca.mandatory()) {
                out.println(String.format("    %s();", getMethodName(ca)));
            }
        });
        out.println("  }");
    }

}
