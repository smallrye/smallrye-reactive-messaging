package org.eclipse.microprofile.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

public class MessageSpecCompatibilityTest {

    public static Class<?> getClassesFromJarFile(File jarFile) throws IOException, ClassNotFoundException {
        // URL classloader without parent
        try (URLClassLoader cl = URLClassLoader.newInstance(
                new URL[] { new URL("jar:file:" + jarFile + "!/") }, null)) {
            return cl.loadClass(Message.class.getName());
        }
    }

    @Test
    void loadClassFromJar() throws IOException, ClassNotFoundException {
        File jarFile = new File("./target/lib/microprofile-reactive-messaging-api.jar");
        Class<?> clazz = getClassesFromJarFile(jarFile);

        List<String> specMethods = Arrays.stream(clazz.getDeclaredMethods())
                .filter(m -> Modifier.isPublic(m.getModifiers()))
                .map(Method::toGenericString)
                .collect(Collectors.toList());

        List<String> rmMethods = Arrays.stream(Message.class.getDeclaredMethods())
                .filter(m -> Modifier.isPublic(m.getModifiers()))
                .map(Method::toGenericString)
                .collect(Collectors.toList());

        System.out.println(Message.class.getName() + " public methods from MicroProfile specification:");
        for (String signature : specMethods) {
            System.out.println(signature);
        }
        assertThat(rmMethods).containsAll(specMethods);
        rmMethods.removeAll(specMethods);
        System.out.println("Remaining methods in Smallrye implementation:");
        for (String signature : rmMethods) {
            System.out.println(signature);
        }
    }
}
