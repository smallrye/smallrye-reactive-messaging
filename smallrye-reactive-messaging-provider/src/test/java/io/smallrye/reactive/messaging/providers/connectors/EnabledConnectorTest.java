package io.smallrye.reactive.messaging.providers.connectors;

import static org.assertj.core.api.Assertions.assertThat;

import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.WeldTestBase;

public class EnabledConnectorTest extends WeldTestBase {

    @BeforeEach
    void setupConfig() {
        // Explicitly enabled.
        installConfig("src/test/resources/config/dummy-connector-config-enabled.properties");
    }

    @Test
    public void test() {
        initializer.addBeanClasses(DummyBean.class);

        initialize();

        assertThat(registry(container).getPublishers("dummy.source")).isNotEmpty();
        assertThat(registry(container).getSubscribers("dummy-sink")).isNotEmpty();

        MyDummyConnector bean = container.select(MyDummyConnector.class, ConnectorLiteral.of("dummy")).get();
        assertThat(bean.list()).containsExactly("8", "10", "12");
        assertThat(bean.gotCompletion()).isTrue();
        assertThat(bean.getConfigs()).hasSize(2).allSatisfy(config -> {
            assertThat(config.getValue("foo", String.class)).isEqualTo("bar");
            assertThat(config.getOptionalValue("foo", String.class)).contains("bar");
        });
    }

}
