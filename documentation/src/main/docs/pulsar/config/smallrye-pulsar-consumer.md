|Attribute | Description | Type   | Config file | Default |
| :---               | :--------------       | :----: | :----:    | :---    |
| *topicNames* | Topic name | Set | true | [] |
| *topicsPattern* | Topic pattern | Pattern | true |  |
| *subscriptionName* | Subscription name | String | true |  |
| *subscriptionType* | Subscription type.<br>Four subscription types are available:<br>* Exclusive<br>* Failover<br>* Shared<br>* Key_Shared | SubscriptionType | true | Exclusive |
| *subscriptionProperties* |  | Map | true |  |
| *subscriptionMode* |  | SubscriptionMode | true | Durable |
| *messageListener* |  | MessageListener | false |  |
| *consumerEventListener* |  | ConsumerEventListener | false |  |
| *negativeAckRedeliveryBackoff* | Interface for custom message is negativeAcked policy. You can specify `RedeliveryBackoff` for a consumer. | RedeliveryBackoff | false |  |
| *ackTimeoutRedeliveryBackoff* | Interface for custom message is ackTimeout policy. You can specify `RedeliveryBackoff` for a consumer. | RedeliveryBackoff | false |  |
| *receiverQueueSize* | Size of a consumer's receiver queue.<br><br>For example, the number of messages accumulated by a consumer before an application calls `Receive`.<br><br>A value higher than the default value increases consumer throughput, though at the expense of more memory utilization. | int | true | 1000 |
| *acknowledgementsGroupTimeMicros* | Group a consumer acknowledgment for a specified time.<br><br>By default, a consumer uses 100ms grouping time to send out acknowledgments to a broker.<br><br>Setting a group time of 0 sends out acknowledgments immediately.<br><br>A longer ack group time is more efficient at the expense of a slight increase in message re-deliveries after a failure. | long | true | 100000 |
| *maxAcknowledgmentGroupSize* | Group a consumer acknowledgment for the number of messages. | int | true | 1000 |
| *negativeAckRedeliveryDelayMicros* | Delay to wait before redelivering messages that failed to be processed.<br><br>When an application uses `Consumer#negativeAcknowledge(Message)`, failed messages are redelivered after a fixed timeout. | long | true | 60000000 |
| *maxTotalReceiverQueueSizeAcrossPartitions* | The max total receiver queue size across partitions.<br><br>This setting reduces the receiver queue size for individual partitions if the total receiver queue size exceeds this value. | int | true | 50000 |
| *consumerName* | Consumer name | String | true |  |
| *ackTimeoutMillis* | Timeout of unacked messages | long | true | 0 |
| *tickDurationMillis* | Granularity of the ack-timeout redelivery.<br><br>Using an higher `tickDurationMillis` reduces the memory overhead to track messages when setting ack-timeout to a bigger value (for example, 1 hour). | long | true | 1000 |
| *priorityLevel* | Priority level for a consumer to which a broker gives more priority while dispatching messages in Shared subscription type.<br><br>The broker follows descending priorities. For example, 0=max-priority, 1, 2,...<br><br>In Shared subscription type, the broker **first dispatches messages to the max priority level consumers if they have permits**. Otherwise, the broker considers next priority level consumers.<br><br>**Example 1**<br>If a subscription has consumerA with `priorityLevel` 0 and consumerB with `priorityLevel` 1, then the broker **only dispatches messages to consumerA until it runs out permits** and then starts dispatching messages to consumerB.<br><br>**Example 2**<br>Consumer Priority, Level, Permits<br>C1, 0, 2<br>C2, 0, 1<br>C3, 0, 1<br>C4, 1, 2<br>C5, 1, 1<br><br>Order in which a broker dispatches messages to consumers is: C1, C2, C3, C1, C4, C5, C4. | int | true | 0 |
| *maxPendingChunkedMessage* | The maximum size of a queue holding pending chunked messages. When the threshold is reached, the consumer drops pending messages to optimize memory utilization. | int | true | 10 |
| *autoAckOldestChunkedMessageOnQueueFull* | Whether to automatically acknowledge pending chunked messages when the threshold of `maxPendingChunkedMessage` is reached. If set to `false`, these messages will be redelivered by their broker. | boolean | true | false |
| *expireTimeOfIncompleteChunkedMessageMillis* | The time interval to expire incomplete chunks if a consumer fails to receive all the chunks in the specified time period. The default value is 1 minute. | long | true | 60000 |
| *cryptoKeyReader* |  | CryptoKeyReader | false |  |
| *messageCrypto* |  | MessageCrypto | false |  |
| *cryptoFailureAction* | Consumer should take action when it receives a message that can not be decrypted.<br>* **FAIL**: this is the default option to fail messages until crypto succeeds.<br>* **DISCARD**:silently acknowledge and not deliver message to an application.<br>* **CONSUME**: deliver encrypted messages to applications. It is the application's responsibility to decrypt the message.<br><br>The decompression of message fails.<br><br>If messages contain batch messages, a client is not be able to retrieve individual messages in batch.<br><br>Delivered encrypted message contains `EncryptionContext` which contains encryption and compression information in it using which application can decrypt consumed message payload. | ConsumerCryptoFailureAction | true | FAIL |
| *properties* | A name or value property of this consumer.<br><br>`properties` is application defined metadata attached to a consumer.<br><br>When getting a topic stats, associate this metadata with the consumer stats for easier identification. | SortedMap | true | {} |
| *readCompacted* | If enabling `readCompacted`, a consumer reads messages from a compacted topic rather than reading a full message backlog of a topic.<br><br>A consumer only sees the latest value for each key in the compacted topic, up until reaching the point in the topic message when compacting backlog. Beyond that point, send messages as normal.<br><br>Only enabling `readCompacted` on subscriptions to persistent topics, which have a single active consumer (like failure or exclusive subscriptions).<br><br>Attempting to enable it on subscriptions to non-persistent topics or on shared subscriptions leads to a subscription call throwing a `PulsarClientException`. | boolean | true | false |
| *subscriptionInitialPosition* | Initial position at which to set cursor when subscribing to a topic at first time. | SubscriptionInitialPosition | true | Latest |
| *patternAutoDiscoveryPeriod* | Topic auto discovery period when using a pattern for topic's consumer.<br><br>The default and minimum value is 1 minute. | int | true | 60 |
| *regexSubscriptionMode* | When subscribing to a topic using a regular expression, you can pick a certain type of topics.<br><br>* **PersistentOnly**: only subscribe to persistent topics.<br>* **NonPersistentOnly**: only subscribe to non-persistent topics.<br>* **AllTopics**: subscribe to both persistent and non-persistent topics. | RegexSubscriptionMode | true | PersistentOnly |
| *deadLetterPolicy* | Dead letter policy for consumers.<br><br>By default, some messages are probably redelivered many times, even to the extent that it never stops.<br><br>By using the dead letter mechanism, messages have the max redelivery count. **When exceeding the maximum number of redeliveries, messages are sent to the Dead Letter Topic and acknowledged automatically**.<br><br>You can enable the dead letter mechanism by setting `deadLetterPolicy`.<br><br>**Example**<br><code><br>client.newConsumer()<br>.deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(10).build())<br>.subscribe();<br></code><br>Default dead letter topic name is `{TopicName}-{Subscription}-DLQ`.<br><br>To set a custom dead letter topic name:<br><code><br>client.newConsumer()<br>.deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(10)<br>.deadLetterTopic("your-topic-name").build())<br>.subscribe();<br></code><br>When specifying the dead letter policy while not specifying `ackTimeoutMillis`, you can set the ack timeout to 30000 millisecond. | DeadLetterPolicy | true |  |
| *retryEnable* |  | boolean | true | false |
| *batchReceivePolicy* |  | BatchReceivePolicy | false |  |
| *autoUpdatePartitions* | If `autoUpdatePartitions` is enabled, a consumer subscribes to partition increasement automatically.<br><br>**Note**: this is only for partitioned consumers. | boolean | true | true |
| *autoUpdatePartitionsIntervalSeconds* |  | long | true | 60 |
| *replicateSubscriptionState* | If `replicateSubscriptionState` is enabled, a subscription state is replicated to geo-replicated clusters. | boolean | true | false |
| *resetIncludeHead* |  | boolean | true | false |
| *keySharedPolicy* |  | KeySharedPolicy | false |  |
| *batchIndexAckEnabled* |  | boolean | true | false |
| *ackReceiptEnabled* |  | boolean | true | false |
| *poolMessages* |  | boolean | true | false |
| *payloadProcessor* |  | MessagePayloadProcessor | false |  |
| *startPaused* |  | boolean | true | false |
| *autoScaledReceiverQueueSizeEnabled* |  | boolean | true | false |
| *topicConfigurations* |  | List | true | [] |
