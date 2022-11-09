package io.smallrye.reactive.messaging.connector;

import jakarta.enterprise.util.AnnotationLiteral;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;

public class ConnectorAttributeLiteral extends AnnotationLiteral<ConnectorAttribute> implements ConnectorAttribute {

    private static final long serialVersionUID = 1L;
    private final String name;
    private final String description;
    private final String type;
    private final boolean mandatory;
    private final Direction direction;
    private String alias = ConnectorAttribute.NO_VALUE;
    private String defaultValue = ConnectorAttribute.NO_VALUE;

    public ConnectorAttributeLiteral(String name, String description, String type, boolean mandatory,
            Direction direction) {
        this.name = name;
        this.description = description;
        this.type = type;
        this.mandatory = mandatory;
        this.direction = direction;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String description() {
        return description;
    }

    @Override
    public boolean hiddenFromDocumentation() {
        return false;
    }

    @Override
    public boolean mandatory() {
        return mandatory;
    }

    @Override
    public Direction direction() {
        return direction;
    }

    @Override
    public String defaultValue() {
        return defaultValue;
    }

    @Override
    public boolean deprecated() {
        return false;
    }

    @Override
    public String alias() {
        return alias;
    }

    @Override
    public String type() {
        return type;
    }

    public ConnectorAttributeLiteral setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public ConnectorAttributeLiteral setDefaultValue(String dv) {
        this.defaultValue = dv;
        return this;
    }
}
