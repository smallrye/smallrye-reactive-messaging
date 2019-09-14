package io.smallrye.reactive.messaging.aws.sns;

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
 * @version 1.0.4
 *
 */
public class SnsClientManager {

    //Instance field initialized once.
    private static final SnsClientManager INSTANCE = new SnsClientManager();

    /**
     * Private constructor
     */
    private SnsClientManager() {

    }

    /**
     * Get instance method of singleton
     * 
     * @return instance of SnsClientManager
     */
    public static SnsClientManager get() {
        return INSTANCE;
    }

    /**
     * Create Async SNS client
     * 
     * @return Async SNS client
     */
    public SnsAsyncClient getAsyncClient(SnsClientConfig cfg) {
        NettySdkAsyncHttpService nettySdkAsyncService = new NettySdkAsyncHttpService();
        SdkAsyncHttpClient nettyHttpClient = nettySdkAsyncService.createAsyncHttpClientFactory().build();
        SnsAsyncClientBuilder clientBuilder = SnsAsyncClient.builder().httpClient(nettyHttpClient);
        if (cfg.isMockSnsTopic()) {
            URL snsUrl;
            try {
                Objects.requireNonNull(cfg.getHost(), "Host cannot be null");
                snsUrl = new URL(cfg.getHost());
                clientBuilder.endpointOverride(snsUrl.toURI());
                clientBuilder.region(Region.AP_SOUTH_1);
            } catch (MalformedURLException | URISyntaxException e) {
                throw new IllegalArgumentException("Invalid URL", e);
            }
        }
        return clientBuilder.build();
    }
}
