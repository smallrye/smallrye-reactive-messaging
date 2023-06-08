///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS org.apache.pulsar:pulsar-client-original:3.0.0
//DEPS com.fasterxml.jackson.core:jackson-annotations:2.15.1
//DEPS io.swagger:swagger-annotations:1.6.2
//DEPS info.picocli:picocli:4.5.0

import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.swagger.annotations.ApiModelProperty;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

/**
 * Script run to write configuration reference for Pulsar client config options.
 * The script loads configuration classes and write a markdown table with the following columns
 * - Attribute
 * - Description
 * - Type
 * - Config file (whether the property is settable from a configuration file
 * - Default value
 * <p>
 * Run with `.github/PulsarConfigDoc.java -d documentation/src/main/docs/pulsar/config`
 * <p>
 */
@CommandLine.Command(name = "pulsar-config-doc", mixinStandardHelpOptions = true, version = "0.1",
        description = "Pulsar Config Object Documentation")
public class PulsarConfigDoc implements Callable<Integer> {

    @CommandLine.Parameters(description = "Pulsar config type", arity = "0..n")
    private ConfigType[] configType = ConfigType.values();

    @CommandLine.Option(names = {"-f", "--file"}, description = { "Whether to write the markdown to file or not" }, required = false, defaultValue = "true")
    private boolean toFile;

    @CommandLine.Option(names = {"-d", "--directory"}, description = { "Target directory to write the markdown file" }, required = false, defaultValue = "./")
    private Path directory;

    enum ConfigType {
        CLIENT(ClientConfigurationData.class),
        CONSUMER(ConsumerConfigurationData.class),
        PRODUCER(ProducerConfigurationData.class);

        Class<?> configClass;
        ConfigType(Class<?> configClass) {
            this.configClass = configClass;
        }

    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new PulsarConfigDoc())
            .setCaseInsensitiveEnumValuesAllowed(true)
            .execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        for (ConfigType type : ConfigType.values()) {
            extractConfigFromConfigType(type);
        }
        return 0;
    }

    private void extractConfigFromConfigType(ConfigType configType) throws NoSuchMethodException, InstantiationException,
            IllegalAccessException, InvocationTargetException, IOException {
                Class<?> configClass = configType.configClass;
        Constructor<?> declaredConstructor = configClass.getDeclaredConstructor();
        declaredConstructor.setAccessible(true);
        var instance = declaredConstructor.newInstance();
        StringBuilder sb = new StringBuilder();
        sb.append("|Attribute | Description | Type   | Config file | Default |").append("\n");
        sb.append("| :---               | :--------------       | :----: | :----:    | :---    |").append("\n");
        for(Field f : configClass.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            f.setAccessible(true);
            ApiModelProperty apiModel = f.getAnnotation(ApiModelProperty.class);
            JsonIgnore ignore = f.getAnnotation(JsonIgnore.class);

            var name = f.getName();
            var type = f.getType().getSimpleName();
            var description = (apiModel == null ? "" : apiModel.value());
            description = sanitizeMarkdown(description);
            var configFile = !(ignore != null && ignore.value());
            var defaultVal = f.get(instance);
            appendLine(sb, name, description, type, Boolean.toString(configFile), (defaultVal == null || !configFile) ? "" : String.valueOf(defaultVal));
        }

        if (toFile) {
            Path path = directory.resolve("smallrye-pulsar-" + configType.name().toLowerCase()+".md");
            System.out.println("Writing " + configType + " documentation to file " + path);
            Files.writeString(path, sb);
        } else {
            System.out.println("Writing " + configType + " documentation to stdout...");
            System.out.println(sb);
        }
    }

    static String sanitizeMarkdown(String description) {
        Pattern linkPattern = Pattern.compile("\\{@link\\s(.*)}");
        Matcher matcher = linkPattern.matcher(description);
        if (matcher.find()) {
            description = matcher.replaceAll("`" + matcher.group(1) + "`");
        }
        description = description.replaceAll("(```java)", "<code>");
        description = description.replaceAll("(```)", "</code>");
        description = description.replaceAll("[\\\n]", "<br>");
        return description;
    }

    static StringBuilder appendLine(StringBuilder sb, String name, String... line) {
        sb.append("| *").append(name).append("* |");
        for (String l : line) {
            if (l == null) {

            }
            sb.append(" ").append(l).append(" |");
        }
        sb.append("\n");
        return sb;
    }


}

