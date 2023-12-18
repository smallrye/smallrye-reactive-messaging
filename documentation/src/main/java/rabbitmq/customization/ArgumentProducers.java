package rabbitmq.customization;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import io.smallrye.common.annotation.Identifier;

@ApplicationScoped
public class ArgumentProducers {
    @Produces
    @Identifier("my-arguments")
    Map<String, Object> customArguments() {
        return Map.of("custom-arg", "value");
    }
}
