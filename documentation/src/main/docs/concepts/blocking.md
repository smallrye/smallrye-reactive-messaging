# @Blocking

The `io.smallrye.reactive.messaging.annotations.Blocking` annotation can
be used on a method annotated with `@Incoming`, or `@Outgoing` to
indicate that the method should be executed on a worker pool:

``` java
@Outgoing("Y")
@Incoming("X")
@Blocking
public String process(String s) {
  return s.toUpperCase();
}
```

If method execution does not need to be ordered, it can be indicated on
the `@Blocking` annotation:

``` java
@Outgoing("Y")
@Incoming("X")
@Blocking(ordered = false)
public String process(String s) {
  return s.toUpperCase();
}
```

When unordered, the invocation can happen concurrently.

By default, use of `@Blocking` results in the method being executed in
the Vert.x worker pool. If it’s desired to execute methods on a custom
worker pool, with specific concurrency needs, it can be defined on
`@Blocking`:

``` java
@Outgoing("Y")
@Incoming("X")
@Blocking("my-custom-pool")
public String process(String s) {
  return s.toUpperCase();
}
```

Specifying the concurrency for the above worker pool requires the
following configuration property to be defined:

    smallrye.messaging.worker.my-custom-pool.max-concurrency=3

## Supported signatures

`@Blocking` does not support every signature. The following table lists
the supported ones.

| Shape      | Signature                                                             | Comment                                                                                                               |
|------------|-----------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| Publisher  | `@Outgoing("in") @Blocking O generator()`                             | Invokes the generator from a worker thread. If `ordered` is set to `false`, the generator can be called concurrently. |
| Publisher  | `@Outgoing("in")  @Blocking  Message<O> generator()`                  | Invokes the generator from a worker thread. If `ordered` is set to `false`, the generator can be called concurrently. |
| Processor  | `@Incoming("in") @Outgoing("bar") @Blocking O process(I in)`          | Invokes the method on a worker thread. If `ordered` is set to `false`, the method can be called concurrently.         |
| Processor  | `@Incoming("in") @Outgoing("bar") @Blocking Message<O> process(I in)` | Invokes the method on a worker thread. If `ordered` is set to `false`, the method can be called concurrently.         |
| Subscriber | `@Incoming("in") @Blocking void consume(I in)`                        | Invokes the method on a worker thread. If `ordered` is set to `false`, the method can be called concurrently.         |
| Subscriber | `@Incoming("in") @Blocking Uni<Void> consume(I in)`                   | Invokes the method on a worker thread. If `ordered` is set to `false`, the method can be called concurrently.         |
| Subscriber | `@Incoming("in") @Blocking CompletionStage<Void> consume(I in)`       | Invokes the method on a worker thread. If `ordered` is set to `false`, the method can be called concurrently.         |

When a method can be called concurrently, the max concurrency depends on
the number of threads from the worker thread pool.

## Using io.smallrye.common.annotation.Blocking

`io.smallrye.common.annotation.Blocking` is another annotation with the
same semantic. `io.smallrye.common.annotation.Blocking` is used by
multiple SmallRye projects and Quarkus.

SmallRye Reactive Messaging also supports
`io.smallrye.common.annotation.Blocking`. However,
`io.smallrye.common.annotation.Blocking` does not allow configuring the
ordering (it defaults to `ordered=true`).

When both annotations are used,
`io.smallrye.reactive.messaging.annotations.Blocking` is preferred.
