Kafka Installation:

Download the Kafka from Apache Kafka Website
Create env variable KAFKA_HOME and Kafka executables into system path.

Kafka requires Zookeeper, so running one or more Zookeeper quorumpeers in required.

Zookeeper is already embedded with Kafka, so no need to download and install separately.

Running Zookeeper:
```
> $KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
```
Running Kafka Broker:
```
> $KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
```
Note: The above installation is very basic for learning Kafka. We need to alter several broker and zookeeper parameters for production use.
