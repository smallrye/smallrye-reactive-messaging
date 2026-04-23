# Claims-Based Security (CBS) Authentication

!!!warning "Experimental"
    CBS authentication support is an experimental feature.

The AMQP connector supports [Claims-Based Security (CBS)](https://docs.oasis-open.org/amqp/amqp-cbs/v1.0/amqp-cbs-v1.0.html) for token-based authentication with AMQP 1.0 brokers.
CBS allows the client to authenticate by sending a security token (such as a JWT or SAS token) to the broker over the AMQP connection, rather than relying solely on SASL mechanisms.
This is commonly used with brokers like **Azure Service Bus**.

## Enabling CBS

To enable CBS authentication on a channel, set the `cbs.enabled` attribute:

```properties
mp.messaging.incoming.my-channel.connector=smallrye-amqp
mp.messaging.incoming.my-channel.host=my-broker.servicebus.windows.net
mp.messaging.incoming.my-channel.port=5671
mp.messaging.incoming.my-channel.use-ssl=true
mp.messaging.incoming.my-channel.cbs.enabled=true
```

CBS can be enabled on both incoming and outgoing channels independently.

## Providing Tokens

When CBS is enabled, the connector requires a CDI bean implementing
{{ javadoc('io.smallrye.reactive.messaging.amqp.cbs.CbsTokenProvider', False, 'io.smallrye.reactive/smallrye-reactive-messaging-amqp') }}.
This bean is called during initial connection and on each token refresh cycle.

``` java
{{ insert('amqp/customization/MyCbsTokenProvider.java', 'provider') }}
```

The `getToken` method receives the channel configuration, allowing you to adapt token generation per channel (e.g., using different audiences or scopes).
It returns a `Uni<CbsToken>` to support non-blocking token retrieval from external identity providers.

### The CbsToken Interface

A `CbsToken` provides:

| Method       | Description                                                                 |
|--------------|-----------------------------------------------------------------------------|
| `token()`    | The token string value (JWT, SAS token, etc.)                               |
| `type()`     | The token type (e.g., `"jwt"` or `"servicebus.windows.net:sastoken"`)       |
| `expiresAt()`| When the token expires                                                      |
| `refreshAt()`| When the token should be refreshed (defaults to `expiresAt()` if not set)   |

The `DefaultCbsToken` record provides a convenient implementation:

```java
// With explicit refresh time (refresh before expiration)
new DefaultCbsToken(token, "jwt", expiresAt, refreshAt);

// Refresh at expiration
new DefaultCbsToken(token, "jwt", expiresAt);

// Using validity duration in seconds
new DefaultCbsToken(token, "jwt", 3600);
```

## Token Refresh

The connector automatically refreshes tokens before they expire.
After a successful authorization, a timer is scheduled based on the `refreshAt()` value of the token.

- If `refreshAt()` is set earlier than `expiresAt()`, the token is refreshed proactively.
- If the refresh fails after retries, the connection is closed and re-established.

Setting `refreshAt()` earlier than `expiresAt()` is recommended to avoid authentication gaps during refresh:

```java
new DefaultCbsToken(
    token, "jwt",
    OffsetDateTime.now().plusHours(1),      // expires in 1 hour
    OffsetDateTime.now().plusMinutes(45));   // refresh after 45 minutes
```

## CBS Exchange Protocols

The connector supports two CBS exchange protocols, configured with `cbs.exchange`:

### `set-token` (default)

The `set-token` protocol sends the token as an AMQP message to the CBS node with:

- Subject: `set-token`
- Application property `token-type`: the token type
- Body: the token string

The broker acknowledges the message to confirm the token was accepted.

```properties
mp.messaging.incoming.my-channel.cbs.exchange=set-token
```

### `put-token`

The `put-token` protocol uses a request-reply exchange, compatible with **Azure Service Bus**.
It sends the token as a request and validates the response status code.

```properties
mp.messaging.incoming.my-channel.cbs.exchange=put-token
mp.messaging.incoming.my-channel.cbs.audience=my-namespace.servicebus.windows.net/my-queue
```

When using `put-token`, the `cbs.audience` attribute sets the token audience.
If not configured, the audience is derived from the broker host and target address in the format `amqp://HOST/ADDRESS`.

## Connection Sharing with CBS

When multiple channels share the same connection using `container-id`, CBS authentication is performed once for the shared connection:

```properties
mp.messaging.incoming.my-channel-in.connector=smallrye-amqp
mp.messaging.incoming.my-channel-in.container-id=shared
mp.messaging.incoming.my-channel-in.cbs.enabled=true

mp.messaging.outgoing.my-channel-out.connector=smallrye-amqp
mp.messaging.outgoing.my-channel-out.container-id=shared
mp.messaging.outgoing.my-channel-out.cbs.enabled=true
```

Both channels share a single connection and a single CBS token authorization.

## Azure Service Bus Example

To connect to Azure Service Bus using CBS with SAS tokens:

``` java
{{ insert('amqp/customization/MySasTokenProvider.java', 'sas-provider') }}
```

```properties
mp.messaging.incoming.my-channel.connector=smallrye-amqp
mp.messaging.incoming.my-channel.host=my-namespace.servicebus.windows.net
mp.messaging.incoming.my-channel.port=5671
mp.messaging.incoming.my-channel.use-ssl=true
mp.messaging.incoming.my-channel.cbs.enabled=true
mp.messaging.incoming.my-channel.cbs.exchange=put-token
mp.messaging.incoming.my-channel.cbs.audience=my-namespace.servicebus.windows.net/my-queue
```

## Retry Configuration

If token authorization fails, the connector retries with exponential backoff.
The retry behavior is controlled by the standard connector retry attributes:

```properties
mp.messaging.incoming.my-channel.retry-on-fail-attempts=6    # max retries (default: 6)
mp.messaging.incoming.my-channel.retry-on-fail-interval=5    # retry delay in seconds (default: 5)
```

If all retries are exhausted, the connection is closed and re-established using the connector's reconnection mechanism.

## Configuration Reference

| Attribute           | Type    | Default                       | Description                                                       |
|---------------------|---------|-------------------------------|-------------------------------------------------------------------|
| `cbs.enabled`       | boolean | `false`                       | Enable CBS authorization                                          |
| `cbs.exchange`      | string  | `set-token`                   | CBS exchange protocol: `set-token` or `put-token`                 |
| `cbs.audience`      | string  |                               | Token audience (auto-derived from address if not set)             |
| `cbs.scopes`        | string  |                               | Token scopes for authorization                                    |
| `cbs.token-manager` | string  | `default-cbs-token-manager`   | CDI bean identifier of a custom `CbsTokenManager.Factory`         |

All attributes are available for both incoming and outgoing channels.
