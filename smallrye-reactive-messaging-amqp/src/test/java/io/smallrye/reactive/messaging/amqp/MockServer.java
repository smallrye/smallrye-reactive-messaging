package io.smallrye.reactive.messaging.amqp;

import java.util.concurrent.ExecutionException;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonServer;
import io.vertx.proton.ProtonServerOptions;

public class MockServer {
    private ProtonServer server;

    // Toggle to (re)use a fixed port, e.g for capture.
    private int bindPort = 0;
    private boolean reuseAddress = false;

    public MockServer(Vertx vertx, Handler<ProtonConnection> connectionHandler)
            throws ExecutionException, InterruptedException {
        this(vertx, connectionHandler, null);
    }

    public MockServer(Vertx vertx, Handler<ProtonConnection> connectionHandler, ProtonServerOptions protonServerOptions)
            throws ExecutionException, InterruptedException {
        if (protonServerOptions == null) {
            protonServerOptions = new ProtonServerOptions();
        }

        protonServerOptions.setReuseAddress(reuseAddress);
        server = ProtonServer.create(vertx, protonServerOptions);
        server.connectHandler(connectionHandler);

        FutureHandler<ProtonServer, AsyncResult<ProtonServer>> handler = FutureHandler.asyncResult();
        server.listen(bindPort, handler);
        handler.get();
    }

    public int actualPort() {
        return server.actualPort();
    }

    public void close() {
        server.close();
    }

    ProtonServer getProtonServer() {
        return server;
    }
}
