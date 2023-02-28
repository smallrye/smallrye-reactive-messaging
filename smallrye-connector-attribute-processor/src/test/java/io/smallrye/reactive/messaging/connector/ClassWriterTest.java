package io.smallrye.reactive.messaging.connector;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;

class ClassWriterTest {

    @Test
    void testPackageName() {
        assertThat(ClassWriter.getPackage("org.acme.Foo")).isEqualTo("org.acme");
        assertThatThrownBy(() -> {
            ClassWriter.getPackage("Foo");
        }).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testConfigClassName() {
        assertThat(ClassWriter.getConfigClassName("org.acme.Foo", "Connector")).isEqualTo("org.acme.FooConnector");
    }

    @Test
    void testSimpleClassName() {
        assertThat(ClassWriter.getSimpleClassName("org.acme.Foo")).isEqualTo("Foo");
        assertThatThrownBy(() -> {
            ClassWriter.getSimpleClassName("Foo");
        }).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testLog() {
        ClassWriter.log("Hello %s", "world");
    }

    @Test
    void testPackageDeclaration() throws UnsupportedEncodingException {
        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        try (PrintWriter writer = new PrintWriter(boas)) {
            ClassWriter.writePackageDeclaration("org.acme", writer);
            writer.flush();
            assertThat(boas.toString("UTF-8")).isEqualTo("package org.acme;\n\n");
        }
    }

    @Test
    void testPackageDeclarationWithNull() throws UnsupportedEncodingException {
        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        try (PrintWriter writer = new PrintWriter(boas)) {
            ClassWriter.writePackageDeclaration(null, writer);
            writer.flush();
            assertThat(boas.toString("UTF-8")).isEmpty();
        }
    }

    @Test
    void testDefaultImportStatements() throws UnsupportedEncodingException {
        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        try (PrintWriter writer = new PrintWriter(boas)) {
            ClassWriter.writeImportStatements(writer);
            writer.flush();
            assertThat(boas.toString("UTF-8")).contains("Config").contains(Optional.class.getName());
        }
    }

    @Test
    void testTargetClassName() {
        assertThat(
                ClassWriter.getTargetDotClassName(
                        new ConnectorAttributeLiteral("a", "a", "boolean", true, ConnectorAttribute.Direction.INCOMING)))
                .isEqualTo("Boolean.class");

        assertThat(
                ClassWriter.getTargetDotClassName(
                        new ConnectorAttributeLiteral("a", "a", "int", true, ConnectorAttribute.Direction.INCOMING)))
                .isEqualTo("Integer.class");

        assertThat(
                ClassWriter.getTargetDotClassName(
                        new ConnectorAttributeLiteral("a", "a", "double", true, ConnectorAttribute.Direction.INCOMING)))
                .isEqualTo("Double.class");

        assertThat(
                ClassWriter.getTargetDotClassName(
                        new ConnectorAttributeLiteral("a", "a", "string", true, ConnectorAttribute.Direction.INCOMING)))
                .isEqualTo("String.class");

        assertThat(
                ClassWriter.getTargetDotClassName(
                        new ConnectorAttributeLiteral("a", "a", "float", true, ConnectorAttribute.Direction.INCOMING)))
                .isEqualTo("Float.class");

        assertThat(
                ClassWriter.getTargetDotClassName(
                        new ConnectorAttributeLiteral("a", "a", "short", true, ConnectorAttribute.Direction.INCOMING)))
                .isEqualTo("Short.class");

        assertThat(
                ClassWriter.getTargetDotClassName(
                        new ConnectorAttributeLiteral("a", "a", "long", true, ConnectorAttribute.Direction.INCOMING)))
                .isEqualTo("Long.class");

        assertThat(
                ClassWriter.getTargetDotClassName(
                        new ConnectorAttributeLiteral("a", "a", "byte", true, ConnectorAttribute.Direction.INCOMING)))
                .isEqualTo("Byte.class");

        assertThat(
                ClassWriter.getTargetDotClassName(
                        new ConnectorAttributeLiteral("a", "a", "Other", true, ConnectorAttribute.Direction.INCOMING)))
                .isEqualTo("Other.class");
    }

    @Test
    void testTargetType() {
        assertThat(
                ClassWriter.getTargetType(
                        new ConnectorAttributeLiteral("a", "a", "boolean", true, ConnectorAttribute.Direction.INCOMING)))
                .isEqualTo("Boolean");

        assertThat(
                ClassWriter.getTargetType(
                        new ConnectorAttributeLiteral("a", "a", "int", true, ConnectorAttribute.Direction.INCOMING)))
                .isEqualTo("Integer");

        assertThat(
                ClassWriter.getTargetType(
                        new ConnectorAttributeLiteral("a", "a", "double", true, ConnectorAttribute.Direction.INCOMING)))
                .isEqualTo("Double");

        assertThat(
                ClassWriter.getTargetType(
                        new ConnectorAttributeLiteral("a", "a", "string", true, ConnectorAttribute.Direction.INCOMING)))
                .isEqualTo("String");

        assertThat(
                ClassWriter.getTargetType(
                        new ConnectorAttributeLiteral("a", "a", "float", true, ConnectorAttribute.Direction.INCOMING)))
                .isEqualTo("Float");

        assertThat(
                ClassWriter.getTargetType(
                        new ConnectorAttributeLiteral("a", "a", "short", true, ConnectorAttribute.Direction.INCOMING)))
                .isEqualTo("Short");

        assertThat(
                ClassWriter.getTargetType(
                        new ConnectorAttributeLiteral("a", "a", "long", true, ConnectorAttribute.Direction.INCOMING)))
                .isEqualTo("Long");

        assertThat(
                ClassWriter.getTargetType(
                        new ConnectorAttributeLiteral("a", "a", "byte", true, ConnectorAttribute.Direction.INCOMING)))
                .isEqualTo("Byte");

        assertThat(
                ClassWriter.getTargetType(
                        new ConnectorAttributeLiteral("a", "a", "Other", true, ConnectorAttribute.Direction.INCOMING)))
                .isEqualTo("Other");
    }

    @Test
    public void testHasAlias() {
        ConnectorAttributeLiteral attribute = new ConnectorAttributeLiteral("a", "a", "Other", true,
                ConnectorAttribute.Direction.INCOMING);
        assertThat(ClassWriter.hasAlias(attribute)).isFalse();
        attribute.setAlias("alias");
        assertThat(ClassWriter.hasAlias(attribute)).isTrue();
    }

    @Test
    public void testHasDefaultValue() {
        ConnectorAttributeLiteral attribute = new ConnectorAttributeLiteral("a", "a", "Other", true,
                ConnectorAttribute.Direction.INCOMING);
        assertThat(ClassWriter.hasDefaultValue(attribute)).isFalse();
        attribute.setDefaultValue("dv");
        assertThat(ClassWriter.hasDefaultValue(attribute)).isTrue();
    }

}
