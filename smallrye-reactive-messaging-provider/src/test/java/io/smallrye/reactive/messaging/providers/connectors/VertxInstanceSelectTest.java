package io.smallrye.reactive.messaging.providers.connectors;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.spi.AfterBeanDiscovery;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.Extension;

import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.vertx.mutiny.core.Vertx;

public class VertxInstanceSelectTest extends WeldTestBaseWithoutTails {

    private static final Properties CONNECTOR_VERTX_CDI_IDENTIFIER_PROPERTIES = new Properties();
    static {
        CONNECTOR_VERTX_CDI_IDENTIFIER_PROPERTIES.setProperty("mp.messaging.connector.vertx.cdi.identifier", "vertx");
    }

    private static final Vertx vertxForCDI = Vertx.vertx();

    // Test the case that 'mp.messaging.connector.vertx.cdi.identifier' is NOT specified and NO Vertx Bean exposed
    // from CDI container. The vertx instance should be internal instance.
    @Test
    public void testNoVertxBeanNoIdentifier() {
        initialize();
        ExecutionHolder executionHolder = get(ExecutionHolder.class);
        assertThat(executionHolder).isNotNull();
        assertThat(executionHolder.vertx()).isNotNull();
        assertThat(executionHolder.vertx()).isNotEqualTo(vertxForCDI);
        assertThat(executionHolder.isInternalVertxInstance()).isTrue();
    }

    // Test the case that 'mp.messaging.connector.vertx.cdi.identifier' IS specified and NO Vertx Bean exposed
    // from CDI container. The vertx instance should be internal instance.
    @Test
    public void testNoVertxBeanWithIdentifier() {
        installConfigFromProperties(CONNECTOR_VERTX_CDI_IDENTIFIER_PROPERTIES);
        initialize();
        ExecutionHolder executionHolder = get(ExecutionHolder.class);
        assertThat(executionHolder).isNotNull();
        assertThat(executionHolder.vertx()).isNotNull();
        assertThat(executionHolder.vertx()).isNotEqualTo(vertxForCDI);
        assertThat(executionHolder.isInternalVertxInstance()).isTrue();
    }

    // Test the case that 'mp.messaging.connector.vertx.cdi.identifier' is NOT specified but has Vertx Bean exposed
    // from CDI container using Default qualifier. The vertx instance should be vertxForCDI.
    @Test
    public void testWithVertxBeanDefaultNoIdentifier() {
        addExtensionClass(VertxCDIDefaultExtension.class);
        initialize();
        ExecutionHolder executionHolder = get(ExecutionHolder.class);
        assertThat(executionHolder).isNotNull();
        assertThat(executionHolder.vertx()).isNotNull();
        assertThat(executionHolder.vertx()).isEqualTo(vertxForCDI);
        assertThat(executionHolder.isInternalVertxInstance()).isFalse();
    }

    // Test the case that 'mp.messaging.connector.vertx.cdi.identifier' IS specified but has Vertx Bean exposed
    // from CDI container using Default qualifier. The vertx instance should be internal instance.
    @Test
    public void testWithVertxBeanDefaultWithIdentifier() {
        installConfigFromProperties(CONNECTOR_VERTX_CDI_IDENTIFIER_PROPERTIES);
        addExtensionClass(VertxCDIDefaultExtension.class);
        initialize();
        ExecutionHolder executionHolder = get(ExecutionHolder.class);
        assertThat(executionHolder).isNotNull();
        assertThat(executionHolder.vertx()).isNotNull();
        assertThat(executionHolder.vertx()).isNotEqualTo(vertxForCDI);
        assertThat(executionHolder.isInternalVertxInstance()).isTrue();
    }

    // Test the case that 'mp.messaging.connector.vertx.cdi.identifier' is specified and has Vertx Bean exposed
    // from CDI container using the Identifier qualifier. The vertx instance should be vertxForCDI.
    @Test
    public void testWithVertxBeanIdentifierWithIdentifier() {
        installConfigFromProperties(CONNECTOR_VERTX_CDI_IDENTIFIER_PROPERTIES);
        addExtensionClass(VertxCDIIdentifierExtension.class);
        initialize();
        ExecutionHolder executionHolder = get(ExecutionHolder.class);
        assertThat(executionHolder).isNotNull();
        assertThat(executionHolder.vertx()).isNotNull();
        assertThat(executionHolder.vertx()).isEqualTo(vertxForCDI);
        assertThat(executionHolder.isInternalVertxInstance()).isFalse();
    }

    public static class VertxCDIDefaultExtension implements Extension {
        public void registerVertxBean(@Observes AfterBeanDiscovery abd, BeanManager beanManager) {
            abd.addBean()
                    .beanClass(Vertx.class).types(Vertx.class)
                    .addQualifiers(Any.Literal.INSTANCE, Default.Literal.INSTANCE)
                    .createWith(cc -> vertxForCDI);
        }
    }

    public static class VertxCDIIdentifierExtension implements Extension {
        public void registerVertxBean(@Observes AfterBeanDiscovery abd, BeanManager beanManager) {
            abd.addBean()
                    .beanClass(Vertx.class).types(Vertx.class)
                    .addQualifiers(Any.Literal.INSTANCE, Identifier.Literal.of("vertx"))
                    .createWith(cc -> vertxForCDI);
        }
    }
}
