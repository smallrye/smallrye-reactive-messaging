package io.smallrye.reactive.messaging.spi;

import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import javax.enterprise.util.AnnotationLiteral;

public final class ConnectorLiteral extends AnnotationLiteral<Connector> implements Connector {

  private static final long serialVersionUID = 1L;

  private final String value;

  public static Connector of(String value) {
    return new ConnectorLiteral(value);
  }

  private ConnectorLiteral(String value) {
    this.value = value;
  }

  public String value() {
    return value;
  }
}

