package io.smallrye.reactive.messaging.aws.sns;

import static io.smallrye.reactive.messaging.aws.sns.i18n.SnsExceptions.ex;
import static io.smallrye.reactive.messaging.aws.sns.i18n.SnsMessages.msg;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Objects;

import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettySdkAsyncHttpService;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.SnsAsyncClientBuilder;

/**
 * Singleton factory class for creating SNSClient
 *
 * @author iabughosh
 */
class SnsClientManager {

    private static final SnsClientManager INSTANCE = new SnsClientManager();

    private SnsAsyncClient client;

    private SnsClientManager() {
        // Avoid direct instantiation.
    }

    /**
     * Get instance method of singleton
     *
     * @return instance of SnsClientManager, cannot be {@code null}
     */
    static SnsClientManager get() {
        return INSTANCE;
    }

    /**
     * Creates the Async SNS client
     *
     * @return the created client.
     */
    synchronized SnsAsyncClient getAsyncClient(SnsClientConfig cfg) {
        if (client != null) {
            // TODO Should be keep a map config -> client?
            return client;
        }
        NettySdkAsyncHttpService nettySdkAsyncService = new NettySdkAsyncHttpService();
        SdkAsyncHttpClient nettyHttpClient = nettySdkAsyncService.createAsyncHttpClientFactory().build();
        SnsAsyncClientBuilder clientBuilder = SnsAsyncClient.builder().httpClient(nettyHttpClient);
        if (cfg.isMockSnsTopic()) {
            URL snsUrl;
            try {
                Objects.requireNonNull(cfg.getHost(), msg.hostNotNull());
                snsUrl = new URL(cfg.getHost());
                clientBuilder.endpointOverride(snsUrl.toURI());
                clientBuilder.region(Region.AP_SOUTH_1);
            } catch (MalformedURLException | URISyntaxException e) {
                throw ex.illegalArgumentInvalidURL(e);
            }
        }
        client = clientBuilder.build();
        return client;
    }

}
