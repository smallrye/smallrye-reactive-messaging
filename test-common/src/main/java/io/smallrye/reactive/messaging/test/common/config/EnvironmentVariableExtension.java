package io.smallrye.reactive.messaging.test.common.config;

import java.lang.reflect.Method;

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.junit.jupiter.api.extension.TestInstancePreDestroyCallback;

import io.smallrye.reactive.messaging.test.common.config.SetEnvironmentVariable.SetEnvironmentVariables;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.properties.SystemProperties;

public class EnvironmentVariableExtension implements TestInstancePostProcessor,
        TestInstancePreDestroyCallback, BeforeEachCallback, ParameterResolver {

    private EnvironmentVariables variables;
    private SystemProperties sysProps;

    @Override
    public void postProcessTestInstance(Object testInstance, ExtensionContext extensionContext) throws Exception {
        sysProps = new SystemProperties();
        sysProps.setup();
        variables = new EnvironmentVariables();
        variables.setup();
    }

    @Override
    public void preDestroyTestInstance(ExtensionContext extensionContext) throws Exception {
        if (variables != null) {
            variables.teardown();
        }
        if (sysProps != null) {
            sysProps.teardown();
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        Class<?> testClass = context.getRequiredTestClass();
        EnvironmentVariables variables = getEnvironmentVariables();
        SystemProperties sysProps = getSystemProperties();

        setVars(testClass.getAnnotation(SetEnvironmentVariables.class), variables);
        setVar(testClass.getAnnotation(SetEnvironmentVariable.class), variables);

        Method method = context.getRequiredTestMethod();
        setVars(method.getAnnotation(SetEnvironmentVariables.class), variables);
        setVar(method.getAnnotation(SetEnvironmentVariable.class), variables);
    }

    private SystemProperties getSystemProperties() {
        return sysProps;
    }

    private EnvironmentVariables getEnvironmentVariables() {
        return variables;
    }

    private void setVar(SetEnvironmentVariable envVar, EnvironmentVariables variables) {
        if (envVar != null) {
            variables.set(envVar.key(), envVar.value());
        }
    }

    private void setVars(SetEnvironmentVariables envVars, EnvironmentVariables variables) {
        if (envVars != null) {
            for (SetEnvironmentVariable envVar : envVars.value()) {
                variables.set(envVar.key(), envVar.value());
            }
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return EnvironmentVariables.class.isAssignableFrom(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return variables;
    }
}
