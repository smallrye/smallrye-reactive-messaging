package io.smallrye.reactive.messaging.connectors;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import javax.enterprise.inject.spi.DeploymentException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.WeldTestBase;

public class ChannelNameConflictTest extends WeldTestBase {

    @BeforeAll
    public static void setupConfig() {
        installConfig("src/test/resources/config/channel-name-conflict.properties");
    }

    @AfterAll
    public static void clear() {
        releaseConfig();
    }

    @Test
    public void test() {
        assertThatThrownBy(() -> {
            initializer.addBeanClasses(DummyBean.class);
            initialize();
        }).isInstanceOf(DeploymentException.class);

    }
}
