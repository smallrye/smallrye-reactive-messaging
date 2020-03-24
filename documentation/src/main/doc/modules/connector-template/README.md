# Connector Template

This _module_ is a template, and should not be linked from anywhere in the web site.
It gives the structure to reproduce when documenting a connector.

It contains a set of `TASKS` and variable ($VARIABLE) that need to be replaced:

* $CONNECTOR-NAME - the connector name (smallrye-kafka)
* $CONNECTOR - the connector simple name (such as Kafka, AMQP...)
* $TECHNOLOGY - the technology full name (Apache Kafka)
* $LINK_TO_UNDERLYING_CLIENT - the link to the underlying technology
* $TYPE_OF_MESSAGE - the type of message (Kafka Records)
* $URL - the mock url
* $DEFAULT_URL - the default url
* $BASE_URL_PROPERTY - the location alias
* $OTHER_ATTRIBUTES - the other mandatory attributes
* $DESTINATION - the type of destination (topic, address, queue)
* $DESTINATION_ATTRIBUTE - the attribute to set the destination
* $INBOUND_METADATA_CLASS - the inbound metadata class
* $OUTBOUND_METADATA_CLASS - the outbound metadata class

Both connector attribute class name must be updated.

The tasks are described using the `TASK:` prefix.
Be aware that some tasks are in the included snippets.

IMPORTANT: The examples must be renamed / prefixed with the technology name to avoid conflicts.
