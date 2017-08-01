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

Kafka has several shell scripts to run several commands related topics, producers, and cosumers.

* connect-distributed.sh
* kafka-replica-verification.sh
* connect-standalone.sh
* kafka-run-class.sh
* kafka-server-start.sh
* kafka-acls.sh
* kafka-server-stop.sh
* kafka-broker-api-versions.sh
* kafka-simple-consumer-shell.sh
* kafka-configs.sh
* kafka-streams-application-reset.sh
* kafka-console-consumer.sh
* kafka-topics.sh
* kafka-console-producer.sh
* kafka-verifiable-consumer.sh
* kafka-consumer-groups.sh
* kafka-verifiable-producer.sh
* kafka-consumer-offset-checker.sh
* kafka-consumer-perf-test.sh
* kafka-mirror-maker.sh
* zookeeper-security-migration.sh
* kafka-preferred-replica-election.sh	
* zookeeper-server-start.sh
* kafka-producer-perf-test.sh
* zookeeper-server-stop.sh
* kafka-reassign-partitions.sh
* zookeeper-shell.sh
* kafka-replay-log-producer.sh


### kafka-topics.sh: 
It is used to create, list, describe, delete and manage kafka topics.

```
1. Listing Kafka topics:
  $KAFKA_HOME/bin/kafka-topics.sh --list --zookeeper localhost:2181

2. Creating Kafka topics:
  $KAFKA_HOME/bin/kafka-topics.sh --create --partitions 5 --replication-factor 1 --topic stocks --zookeeper localhost:2181
```
