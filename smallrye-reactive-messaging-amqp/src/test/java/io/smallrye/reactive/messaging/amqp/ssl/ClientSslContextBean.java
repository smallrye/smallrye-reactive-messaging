package io.smallrye.reactive.messaging.amqp.ssl;

import static io.smallrye.reactive.messaging.amqp.ssl.KeyStores.CLIENT_TS_PASSWORD;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import io.smallrye.common.annotation.Identifier;

@ApplicationScoped
public class ClientSslContextBean {

    private final SSLContext mySslContext;

    ClientSslContextBean() {
        try {
            Path tsPath = Paths.get(KeyStores.getClientTsUrl().toURI());
            TrustManagerFactory tmFactory;
            try (InputStream trustStoreStream = Files.newInputStream(tsPath, StandardOpenOption.READ)) {
                KeyStore trustStore = KeyStore.getInstance("pkcs12");
                trustStore.load(trustStoreStream, CLIENT_TS_PASSWORD.toCharArray());
                tmFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmFactory.init(trustStore);
            }

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(
                    null,
                    tmFactory.getTrustManagers(),
                    null);

            mySslContext = sslContext;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Produces
    @Identifier("mysslcontext")
    SSLContext getSSLContext() {
        return mySslContext;
    }
}
