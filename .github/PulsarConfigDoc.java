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
 * Script run after the release.
 * The script does the following action in this order:
 * - checks that the previous milestone is closed, close it if not
 * - checks that the next milestone exists, or create it if not
 * - creates the Github release and compute the release notes
 * <p>
 * Run with `./PostRelease.java --token=GITHUB_TOKEN --release-version=version
 * <p>
 * 1. The github token is mandatory.
 * <p>
 * The version is taken from the last tag if not set.
 */
@CommandLine.Command(name = "pulsar-config-doc", mixinStandardHelpOptions = true, version = "0.1",
        description = "Pulsar Config Object Documentation")
public class PulsarConfigDoc implements Callable<Integer> {

    @CommandLine.Parameters(description = "The config type", arity = "0..n")
    private ConfigType[] configType = ConfigType.values();

    @CommandLine.Option(names = {"-f", "--file"}, required = false, defaultValue = "true")
    private boolean toFile;

    @CommandLine.Option(names = {"-d", "--directory"}, required = false, defaultValue = "./")
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
            Files.writeString(directory.resolve("smallrye-pulsar-" + configType.name().toLowerCase()+".md"), sb);
        } else {
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

