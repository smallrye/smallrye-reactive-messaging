package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPExceptions.ex;
import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;

import java.util.Optional;

import javax.net.ssl.SSLContext;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.smallrye.reactive.messaging.ClientCustomizer;
import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;
import io.vertx.amqp.AmqpClientOptions;
import io.vertx.core.net.JdkSSLEngineOptions;
import io.vertx.core.spi.tls.SslContextFactory;

@ApplicationScoped
public class SslContextClientCustomizer implements ClientCustomizer<AmqpClientOptions> {

    @Inject
    @Any
    private Instance<SSLContext> clientSslContexts;

    @Override
    public AmqpClientOptions customize(String channel, Config channelConfig, AmqpClientOptions config) {
        AmqpConnectorCommonConfiguration commonConfiguration = new AmqpConnectorCommonConfiguration(channelConfig);
        Optional<String> clientSslContextName = commonConfiguration.getClientSslContextName();
        if (clientSslContextName.isPresent()) {
            SSLContext sslContext = CDIUtils.getInstanceById(clientSslContexts, clientSslContextName.get(), () -> null);
            if (sslContext != null) {
                try {
                    config.setSslEngineOptions(new JdkSSLEngineOptions() {
                        @Override
                        public SslContextFactory sslContextFactory() {
                            return new SslContextFactory() {
                                @Override
                                public SslContext create() {
                                    return new JdkSslContext(
                                            sslContext,
                                            true,
                                            null,
                                            IdentityCipherSuiteFilter.INSTANCE,
                                            ApplicationProtocolConfig.DISABLED,
                                            io.netty.handler.ssl.ClientAuth.NONE,
                                            null,
                                            false);
                                }
                            };
                        }
                    });
                } catch (Exception e) {
                    log.unableToCreateClient(e);
                    throw ex.illegalStateUnableToCreateClient(e);
                }
            }
        }
        return config;
    }
}
