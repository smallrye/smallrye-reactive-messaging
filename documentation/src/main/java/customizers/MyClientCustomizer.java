package customizers;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.Config;

import io.smallrye.reactive.messaging.ClientCustomizer;

@ApplicationScoped
public class MyClientCustomizer implements ClientCustomizer<ClientConfig> {
    @Override
    public ClientConfig customize(String channel, Config channelConfig, ClientConfig config) {
        // customize the client configuration
        return config;
    }

}
