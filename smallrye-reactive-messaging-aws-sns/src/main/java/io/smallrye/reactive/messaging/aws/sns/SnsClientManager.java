package io.smallrye.reactive.messaging.aws.sns;

import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettySdkAsyncHttpService;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

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
     * @returnAsync SNS client
     */
    public SnsAsyncClient getAsyncClient() {
        NettySdkAsyncHttpService nettySdkAsyncService = new NettySdkAsyncHttpService();
        SdkAsyncHttpClient nettyHttpClient = nettySdkAsyncService.createAsyncHttpClientFactory().build();
        return SnsAsyncClient.builder().httpClient(nettyHttpClient).build();
    }
}
