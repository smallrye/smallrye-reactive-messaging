package io.smallrye.reactive.messaging.aws.sqs.client;

import java.net.URI;
import java.net.URISyntaxException;

import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorCommonConfiguration;
import io.vertx.core.impl.EventLoopContext;
import io.vertx.core.impl.VertxInternal;
import io.vertx.mutiny.core.Vertx;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClientBuilder;

/**
 * Based on some logic of the quarkiverse lib for aws
 * TODO: Not sure about all of this. Maybe just qualified SqsAsyncClient beans? This would allow to use it with
 * Quarkiverse extension. Maybe. Or manually created clients. Otherwise, I would be forced to forward all possible
 * configurations. But sometimes configs are not flexible enough. The credentials provider topic is already
 * providing a lot of different approaches to configure them.
 * But using something like Quarkiverse would also require that we check if netty eventloop is used or not.
 */
public class SqsClientFactory {

    public static final String NETTY_HTTP_SERVICE = "software.amazon.awssdk.http.nio.netty.NettySdkAsyncHttpService";

    // TODO: I think this makes no sense when we already use netty in vertx.
    public static final String AWS_CRT_HTTP_SERVICE = "software.amazon.awssdk.http.crt.AwsCrtSdkHttpService";

    public static SqsAsyncClient createSqsClient(SqsConnectorCommonConfiguration config, Vertx vertx) {

        final SqsAsyncClientBuilder builder = SqsAsyncClient.builder();

        final EventLoopContext context = ((VertxInternal) vertx.getDelegate()).createEventLoopContext();

        if (nettyExists()) {
            builder.asyncConfiguration(b -> b.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
                    context.nettyEventLoop().parent())).httpClientBuilder(NettyNioAsyncHttpClient.builder()
                            .eventLoopGroup(SdkEventLoopGroup.create(context.nettyEventLoop().parent())));
        }

        config.getEndpointOverride().ifPresent(endpointOverride -> {
            try {
                builder.endpointOverride(new URI(endpointOverride));
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        });

        config.getRegion().ifPresent(region -> builder.region(Region.of(region)));

        // TODO: Yeah this is where it starts to get ugly. There are so many different ways to configure it
        //  Sometimes you even might want to do role chaining etc, which makes it even more complicated.
        //  I would basically recreate Quarkiverses approach here. Or maybe inject the client directly?
        builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")));

        return builder.build();
    }

    public static boolean nettyExists() {
        try {
            Class.forName(NETTY_HTTP_SERVICE);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean awsCrtExists() {
        try {
            Class.forName(AWS_CRT_HTTP_SERVICE);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
