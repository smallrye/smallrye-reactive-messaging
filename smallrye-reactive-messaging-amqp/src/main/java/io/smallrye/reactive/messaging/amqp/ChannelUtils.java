package io.smallrye.reactive.messaging.amqp;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ChannelUtils {

    public static List<String> getClientCapabilities(AmqpConnectorCommonConfiguration configuration) {
        if (configuration.getCapabilities().isPresent()) {
            String capabilities = configuration.getCapabilities().get();
            return Arrays.stream(capabilities.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

}
