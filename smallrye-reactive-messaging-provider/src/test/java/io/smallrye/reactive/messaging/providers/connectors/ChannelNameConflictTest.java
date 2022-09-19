package io.smallrye.reactive.messaging.providers.connectors;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.enterprise.inject.spi.DeploymentException;

import org.junit.jupiter.api.*;

import io.smallrye.reactive.messaging.WeldTestBase;

public class ChannelNameConflictTest extends WeldTestBase {

    @BeforeEach
    void setupConfig() {
        installConfig("src/test/resources/config/channel-name-conflict.properties");
    }

    @Test
    public void test() {
        assertThatThrownBy(() -> {
            initializer.addBeanClasses(DummyBean.class);
            initialize();
        }).isInstanceOf(DeploymentException.class);

    }
}
