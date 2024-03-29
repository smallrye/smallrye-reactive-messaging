|Attribute | Description | Type   | Config file | Default |
| :---               | :--------------       | :----: | :----:    | :---    |
| *serviceUrl* | Pulsar cluster HTTP URL to connect to a broker. | String | true |  |
| *serviceUrlProvider* | The implementation class of ServiceUrlProvider used to generate ServiceUrl. | ServiceUrlProvider | false |  |
| *authentication* | Authentication settings of the client. | Authentication | false |  |
| *authPluginClassName* | Class name of authentication plugin of the client. | String | true |  |
| *authParams* | Authentication parameter of the client. | String | true |  |
| *authParamMap* | Authentication map of the client. | Map | true |  |
| *operationTimeoutMs* | Client operation timeout (in milliseconds). | long | true | 30000 |
| *lookupTimeoutMs* | Client lookup timeout (in milliseconds). | long | true | -1 |
| *statsIntervalSeconds* | Interval to print client stats (in seconds). | long | true | 60 |
| *numIoThreads* | Number of IO threads. | int | true | 10 |
| *numListenerThreads* | Number of consumer listener threads. | int | true | 10 |
| *connectionsPerBroker* | Number of connections established between the client and each Broker. A value of 0 means to disable connection pooling. | int | true | 1 |
| *connectionMaxIdleSeconds* | Release the connection if it is not used for more than [connectionMaxIdleSeconds] seconds. If  [connectionMaxIdleSeconds] < 0, disabled the feature that auto release the idle connections | int | true | 180 |
| *useTcpNoDelay* | Whether to use TCP NoDelay option. | boolean | true | true |
| *useTls* | Whether to use TLS. | boolean | true | false |
| *tlsKeyFilePath* | Path to the TLS key file. | String | true |  |
| *tlsCertificateFilePath* | Path to the TLS certificate file. | String | true |  |
| *tlsTrustCertsFilePath* | Path to the trusted TLS certificate file. | String | true |  |
| *tlsAllowInsecureConnection* | Whether the client accepts untrusted TLS certificates from the broker. | boolean | true | false |
| *tlsHostnameVerificationEnable* | Whether the hostname is validated when the client creates a TLS connection with brokers. | boolean | true | false |
| *concurrentLookupRequest* | The number of concurrent lookup requests that can be sent on each broker connection. Setting a maximum prevents overloading a broker. | int | true | 5000 |
| *maxLookupRequest* | Maximum number of lookup requests allowed on each broker connection to prevent overloading a broker. | int | true | 50000 |
| *maxLookupRedirects* | Maximum times of redirected lookup requests. | int | true | 20 |
| *maxNumberOfRejectedRequestPerConnection* | Maximum number of rejected requests of a broker in a certain time frame (60 seconds) after the current connection is closed and the client creating a new connection to connect to a different broker. | int | true | 50 |
| *keepAliveIntervalSeconds* | Seconds of keeping alive interval for each client broker connection. | int | true | 30 |
| *connectionTimeoutMs* | Duration of waiting for a connection to a broker to be established.If the duration passes without a response from a broker, the connection attempt is dropped. | int | true | 10000 |
| *requestTimeoutMs* | Maximum duration for completing a request. | int | true | 60000 |
| *readTimeoutMs* | Maximum read time of a request. | int | true | 60000 |
| *autoCertRefreshSeconds* | Seconds of auto refreshing certificate. | int | true | 300 |
| *initialBackoffIntervalNanos* | Initial backoff interval (in nanosecond). | long | true | 100000000 |
| *maxBackoffIntervalNanos* | Max backoff interval (in nanosecond). | long | true | 60000000000 |
| *enableBusyWait* | Whether to enable BusyWait for EpollEventLoopGroup. | boolean | true | false |
| *listenerName* | Listener name for lookup. Clients can use listenerName to choose one of the listeners as the service URL to create a connection to the broker as long as the network is accessible."advertisedListeners" must enabled in broker side. | String | true |  |
| *useKeyStoreTls* | Set TLS using KeyStore way. | boolean | true | false |
| *sslProvider* | The TLS provider used by an internal client to authenticate with other Pulsar brokers. | String | true |  |
| *tlsKeyStoreType* | TLS KeyStore type configuration. | String | true | JKS |
| *tlsKeyStorePath* | Path of TLS KeyStore. | String | true |  |
| *tlsKeyStorePassword* | Password of TLS KeyStore. | String | true |  |
| *tlsTrustStoreType* | TLS TrustStore type configuration. You need to set this configuration when client authentication is required. | String | true | JKS |
| *tlsTrustStorePath* | Path of TLS TrustStore. | String | true |  |
| *tlsTrustStorePassword* | Password of TLS TrustStore. | String | true |  |
| *tlsCiphers* | Set of TLS Ciphers. | Set | true | [] |
| *tlsProtocols* | Protocols of TLS. | Set | true | [] |
| *memoryLimitBytes* | Limit of client memory usage (in byte). The 64M default can guarantee a high producer throughput. | long | true | 67108864 |
| *proxyServiceUrl* | URL of proxy service. proxyServiceUrl and proxyProtocol must be mutually inclusive. | String | true |  |
| *proxyProtocol* | Protocol of proxy service. proxyServiceUrl and proxyProtocol must be mutually inclusive. | ProxyProtocol | true |  |
| *enableTransaction* | Whether to enable transaction. | boolean | true | false |
| *clock* |  | Clock | false |  |
| *dnsLookupBindAddress* | The Pulsar client dns lookup bind address, default behavior is bind on 0.0.0.0 | String | true |  |
| *dnsLookupBindPort* | The Pulsar client dns lookup bind port, takes effect when dnsLookupBindAddress is configured, default value is 0. | int | true | 0 |
| *socks5ProxyAddress* | Address of SOCKS5 proxy. | InetSocketAddress | true |  |
| *socks5ProxyUsername* | User name of SOCKS5 proxy. | String | true |  |
| *socks5ProxyPassword* | Password of SOCKS5 proxy. | String | true |  |
| *description* | The extra description of the client version. The length cannot exceed 64. | String | true |  |
