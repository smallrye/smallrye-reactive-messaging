package io.smallrye.reactive.messaging.amqp.ssl;

import java.net.URL;

import org.jetbrains.annotations.Nullable;

public class KeyStores {
    static final String CLIENT_TS_LOCATION = "/ssl/client.truststore.p12";
    static final String CLIENT_TS_PASSWORD = "clientts";
    static final String SERVER_KS_LOCATION = "/ssl/server.keystore.p12";
    static final String SERVER_KS_PASSWORD = "serverks";

    public static URL getClientTsUrl() {
        return getUrl(CLIENT_TS_LOCATION);
    }

    public static URL getServerKsUrl() {
        return getUrl(SERVER_KS_LOCATION);
    }

    @Nullable
    private static URL getUrl(String path) {
        return KeyStores.class.getResource(path);
    }
}
