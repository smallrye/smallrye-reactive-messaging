package io.smallrye.reactive.messaging.providers.helpers;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import io.vertx.core.Context;
import io.vertx.core.Vertx;

// TODO move to smallrye-common-vertx-context
public class VertxContext {

    public static Context getRootContext(Context context) {
        return io.smallrye.common.vertx.VertxContext.getRootContext(context);
    }

    public static Context createNewDuplicatedContext() {
        return io.smallrye.common.vertx.VertxContext.createNewDuplicatedContext();
    }

    public static void executeBlocking(Context context, Runnable runnable) {
        executeBlocking(context, runnable, true);
    }

    public static void executeBlocking(Context context, Runnable runnable, boolean ordered) {
        context.executeBlocking(Executors.callable(runnable), ordered);
    }

    public static void runOnContext(Context context, Runnable runnable) {
        if (Vertx.currentContext() == context) {
            runnable.run();
        } else {
            context.runOnContext(x -> runnable.run());
        }
    }

    public static void runOnEventLoopContext(Context context, Runnable runnable) {
        if (Vertx.currentContext() == context && Context.isOnEventLoopThread()) {
            runnable.run();
        } else {
            context.runOnContext(x -> runnable.run());
        }
    }

    public static <V> CompletionStage<V> runOnContext(Context context, Consumer<CompletableFuture<V>> runnable) {
        CompletableFuture<V> future = new CompletableFuture<>();
        runOnContext(context, () -> runnable.accept(future));
        return future;
    }

    public static <V> CompletionStage<V> runOnEventLoopContext(Context context, Consumer<CompletableFuture<V>> runnable) {
        CompletableFuture<V> future = new CompletableFuture<>();
        runOnEventLoopContext(context, () -> runnable.accept(future));
        return future;
    }

    public static <V> CompletionStage<V> callOnContext(Context context, Callable<V> callable) {
        return runOnContext(context, future -> {
            try {
                future.complete(callable.call());
            } catch (Throwable reason) {
                future.completeExceptionally(reason);
            }
        });
    }
}
