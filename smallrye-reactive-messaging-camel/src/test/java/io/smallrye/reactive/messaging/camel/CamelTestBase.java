package io.smallrye.reactive.messaging.camel;

import java.util.Arrays;

import org.apache.camel.CamelContext;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Before;

public class CamelTestBase {

    private Weld weld;
    private WeldContainer container;

    @Before
    public void init() {
        weld = new Weld();
    }

    @After
    public void cleanUp() {
        weld.shutdown();
    }

    public void initialize() {
        container = weld.initialize();
    }

    public CamelTestBase addClasses(Class... classes) {
        Arrays.stream(classes).forEach(weld::addBeanClass);
        return this;
    }

    public CamelContext camelContext() {
        return bean(CamelContext.class);
    }

    public <T> T bean(Class<T> clazz) {
        return container.getBeanManager().createInstance().select(clazz).get();
    }

}
