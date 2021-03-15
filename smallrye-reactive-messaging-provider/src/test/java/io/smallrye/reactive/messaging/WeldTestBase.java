package io.smallrye.reactive.messaging;

import org.junit.jupiter.api.BeforeEach;

public class WeldTestBase extends WeldTestBaseWithoutTails {

    @BeforeEach
    public void setUp() {
        super.setUp();
        initializer.addBeanClasses(MyCollector.class);
    }

}
