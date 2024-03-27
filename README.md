# Kafka Connect sink connector for IBM MQ

kafka-connect-mq-sink is a [Kafka Connect](http://kafka.apache.org/documentation.html#connect) sink connector for copying data from Apache Kafka into IBM MQ.

The connector is supplied as source code which you can easily build into a JAR file.

**Note**: A source connector for IBM MQ is also available on [GitHub](https://github.com/ibm-messaging/kafka-connect-mq-source).

## Contents

- [Building the connector](#building-the-connector)
- [Running the connector](#running-the-connector)
- [Running the connector with Docker](#running-with-docker)
- [Deploying the connector to Kubernetes](#deploying-to-kubernetes)
- [Data formats](#data-formats)
- [Security](#security)
- [Configuration](#configuration)
- [Exactly-once message delivery semantics](#exactly-once-message-delivery-semantics)
- [Troubleshooting](#troubleshooting)
- [Support](#support)
- [Issues and contributions](#issues-and-contributions)
- [License](#license)

## Building the connector

To build the connector, you must have the following installed:

- [git](https://git-scm.com/)
- [Maven 3.0 or later](https://maven.apache.org)
- Java 8 or later

Clone the repository with the following command:

```shell
git clone https://github.com/ibm-messaging/kafka-connect-mq-sink.git
```

Change directory into the `kafka-connect-mq-sink` directory:

```shell
cd kafka-connect-mq-sink
```

Run the unit tests:

```shell
mvn test
```

Run the integration tests (requires Docker):

```shell
mvn integration-test
```

Build the connector using Maven:

```shell
mvn clean package
```

Once built, the output is a single JAR `target/kafka-connect-mq-sink-<version>-jar-with-dependencies.jar` which contains all of the required dependencies.

**NOTE:** With the 2.0.0 release the base Kafka Connect library has been updated from 2.6.0 to 3.4.1.

## Running the connector

For step-by-step instructions, see the following guides for running the connector:

- connecting to Apache Kafka [running locally](UsingMQwithKafkaConnect.md)
- connecting to an installation of [IBM Event Streams](https://ibm.github.io/event-streams/connecting/mq/sink)

To run the connector, you must have:

- The JAR from building the connector
- A properties file containing the configuration for the connector
- Apache Kafka 2.0.0 or later, either standalone or included as part of an offering such as IBM Event Streams.
- IBM MQ v9 or later, or the IBM MQ on Cloud service

The connector can be run in a Kafka Connect worker in either standalone (single process) or distributed mode. It's a good idea to start in standalone mode.

### Running in standalone mode

You need two configuration files, one for the configuration that applies to all of the connectors such as the Kafka bootstrap servers, and another for the configuration specific to the MQ sink connector such as the connection information for your queue manager. For the former, the Kafka distribution includes a file called `connect-standalone.properties` that you can use as a starting point. For the latter, you can use `config/mq-sink.properties` in this repository.

The connector connects to MQ using either a client or a bindings connection. For a client connection, you must provide the name of the queue manager, the connection name (one or more host/port pairs) and the channel name. In addition, you can provide a user name and password if the queue manager is configured to require them for client connections. If you look at the supplied `config/mq-sink.properties`, you'll see how to specify the configuration required. For a bindings connection, you must provide the name of the queue manager and also run the Kafka Connect worker on the same system as the queue manager.

To run the connector in standalone mode from the directory into which you installed Apache Kafka, you use a command like this:

```shell
bin/connect-standalone.sh connect-standalone.properties mq-sink.properties
```

### Running in distributed mode

You need an instance of Kafka Connect running in distributed mode. The Kafka distribution includes a file called `connect-distributed.properties` that you can use as a starting point, or follow [Running with Docker](#running-with-docker) or [Deploying to Kubernetes](#deploying-to-kubernetes).

To start the MQ connector, you can use `config/mq-sink.json` in this repository after replacing all placeholders and use a command like this:

```shell
curl -X POST -H "Content-Type: application/json" http://localhost:8083/connectors \
  --data "@./config/mq-sink.json"
```

## Running with Docker

This repository includes an example Dockerfile to run Kafka Connect in distributed mode. It also adds in the MQ sink connector as an available connector plugin. It uses the default `connect-distributed.properties` and `connect-log4j.properties` files.

Before running the Docker commands, make sure to replace `<TAG>` with the desired tag value (e.g., `v2.0.0`, `latest`, etc.) for the Docker image.

To run the Kafka Connect with MQ sink connector using Docker, follow these steps:

1. `mvn clean package`
1. `docker build -t kafkaconnect-with-mq-sink:<TAG> .`
1. `docker run -p 8083:8083 kafkaconnect-with-mq-sink:<TAG>`

**NOTE:** To provide custom properties files create a folder called `config` containing the `connect-distributed.properties` and `connect-log4j.properties` files and use a Docker volume to make them available when running the container like this:

``` shell
docker run -v $(pwd)/config:/opt/kafka/config -p 8083:8083 kafkaconnect-with-mq-sink:<TAG>
```

To start the MQ connector, you can use `config/mq-sink.json` in this repository after replacing all placeholders and use a command like this:

```shell
curl -X POST -H "Content-Type: application/json" http://localhost:8083/connectors \
  --data "@./config/mq-sink.json"
```

## Deploying to Kubernetes

This repository includes a Kubernetes yaml file called `kafka-connect.yaml`. This will create a deployment to run Kafka Connect in distributed mode and a service to access the deployment.

The deployment assumes the existence of a Secret called `connect-distributed-config` and a ConfigMap called `connect-log4j-config`. These can be created using the default files in your Kafka install, however it is easier to edit them later if comments and whitespaces are trimmed before creation.

### Creating Kafka Connect configuration Secret and ConfigMap

Create Secret for Kafka Connect configuration:

1. `cp kafka/config/connect-distributed.properties connect-distributed.properties.orig`
1. `sed '/^#/d;/^[[:space:]]*$/d' < connect-distributed.properties.orig > connect-distributed.properties`
1. `kubectl -n <namespace> create secret generic connect-distributed-config --from-file=connect-distributed.properties`

Create ConfigMap for Kafka Connect Log4j configuration:

1. `cp kafka/config/connect-log4j.properties connect-log4j.properties.orig`
1. `sed '/^#/d;/^[[:space:]]*$/d' < connect-log4j.properties.orig > connect-log4j.properties`
1. `kubectl -n <namespace> create configmap connect-log4j-config --from-file=connect-log4j.properties`

### Creating Kafka Connect deployment and service in Kubernetes

**NOTE:** You will need to [build the Docker image](#running-with-docker) and push it to your Kubernetes image repository. Remember that the supplied Dockerfile is just an example and you will have to modify it for your needs. You might need to update the image name in the `kafka-connect.yaml` file.

1. Update the namespace in `kafka-connect.yaml`
1. `kubectl -n <namespace> apply -f kafka-connect.yaml`
1. `curl <serviceIP>:<servicePort>/connector-plugins` to see whether the MQ sink connector is available to use

### Deploying to OpenShift using Strimzi

This repository includes a Kubernetes yaml file called `strimzi.kafkaconnector.yaml` for use with the [Strimzi](https://strimzi.io) operator. Strimzi provides a simplified way of running the Kafka Connect distributed worker, by defining either a KafkaConnect resource or a KafkaConnectS2I resource.

The KafkaConnectS2I resource provides a nice way to have OpenShift do all the work of building the Docker images for you. This works particularly nicely combined with the KafkaConnector resource that represents an individual connector.

The following instructions assume you are running on OpenShift and have Strimzi 0.16 or later installed.

#### Start a Kafka Connect cluster using KafkaConnectS2I

1. Create a file called `kafka-connect-s2i.yaml` containing the definition of a KafkaConnectS2I resource. You can use the examples in the Strimzi project to get started.
1. Configure it with the information it needs to connect to your Kafka cluster. You must include the annotation `strimzi.io/use-connector-resources: "true"` to configure it to use KafkaConnector resources so you can avoid needing to call the Kafka Connect REST API directly.
1. `oc apply -f kafka-connect-s2i.yaml` to create the cluster, which usually takes several minutes.

#### Add the MQ sink connector to the cluster

1. `mvn clean package` to build the connector JAR.
1. `mkdir my-plugins`
1. `cp target/kafka-connect-mq-sink-*-jar-with-dependencies.jar my-plugins`
1. `oc start-build <kafkaconnectClusterName>-connect --from-dir ./my-plugins` to add the MQ sink connector to the Kafka Connect distributed worker cluster. Wait for the build to complete, which usually takes a few minutes.
1. `oc describe kafkaconnects2i <kafkaConnectClusterName>` to check that the MQ sink connector is in the list of available connector plugins.

#### Start an instance of the MQ sink connector using KafkaConnector

1. `cp deploy/strimzi.kafkaconnector.yaml kafkaconnector.yaml`
1. Update the `kafkaconnector.yaml` file to replace all of the values in `<>`, adding any additional configuration properties.
1. `oc apply -f kafkaconnector.yaml` to start the connector.
1. `oc get kafkaconnector` to list the connectors. You can use `oc describe` to get more details on the connector, such as its status.

## Data formats

Kafka Connect is very flexible but it's important to understand the way that it processes messages to end up with a reliable system. When the connector encounters a message that it cannot process, it stops rather than throwing the message away. Therefore, you need to make sure that the configuration you use can handle the messages the connector will process.

Each message in Kafka Connect is associated with a representation of the message format known as a *schema*. Each Kafka message actually has two parts, key and value, and each part has its own schema. The MQ sink connector does not currently use message keys, but some of the configuration options use the word *Value* because they refer to the Kafka message value.

When the MQ sink connector reads a message from Kafka, it is processed using a *converter* which chooses a schema to represent the message format and creates a Java object containing the message value. The MQ sink connector then converts this internal format into the message it sends to MQ using a *message builder*.

There are three converters built into Apache Kafka. The following table shows which converters to use based on the incoming message encoding.

| Incoming Kafka message | Converter class                                        |
| ---------------------- | ------------------------------------------------------ |
| Any                    | org.apache.kafka.connect.converters.ByteArrayConverter |
| String                 | org.apache.kafka.connect.storage.StringConverter       |
| JSON, may have schema  | org.apache.kafka.connect.json.JsonConverter            |

There are three message builders supplied with the connector, although you can write your own. The basic rule is that if you're using a converter that uses a very simple schema, the default message builder is probably the best choice. If you're using a converter that uses richer schemas to represent complex messages, the JSON message builder is good for generating a JSON representation of the complex data. The following table shows some likely combinations.

| Converter class                                        | Message builder class                                              | Outgoing MQ message    |
| ------------------------------------------------------ | ------------------------------------------------------------------ | ---------------------- |
| org.apache.kafka.connect.converters.ByteArrayConverter | com.ibm.eventstreams.connect.mqsink.builders.DefaultMessageBuilder | **Binary data**        |
| org.apache.kafka.connect.storage.StringConverter       | com.ibm.eventstreams.connect.mqsink.builders.DefaultMessageBuilder | **String data**        |
| org.apache.kafka.connect.json.JsonConverter            | com.ibm.eventstreams.connect.mqsink.builders.JsonMessageBuilder    | **JSON, no schema**    |

When you set *mq.message.body.jms=true*, the MQ messages are generated as JMS messages. This is appropriate if the applications receiving the messages are themselves using JMS.

There's no single configuration that will always be right, but here are some high-level suggestions.

- Message values are treated as byte arrays, pass byte array into MQ message

```shell
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
```

- Message values are treated as strings, pass string into MQ message

```shell
value.converter=org.apache.kafka.connect.storage.StringConverter
```

### The gory detail

The messages received from Kafka are processed by a converter which chooses a schema to represent the message and creates a Java object containing the message value. There are three basic converters built into Apache Kafka.

| Converter class                                        | Kafka message encoding | Value schema        | Value class        |
| ------------------------------------------------------ | ---------------------- | ------------------- | ------------------ |
| org.apache.kafka.connect.converters.ByteArrayConverter | Any                    | OPTIONAL_BYTES      | byte[]             |
| org.apache.kafka.connect.storage.StringConverter       | String                 | OPTIONAL_STRING     | java.lang.String   |
| org.apache.kafka.connect.json.JsonConverter            | JSON, may have schema  | Depends on message  | Depends on message |

The MQ sink connector uses a message builder to build the MQ messsages from the schema and value. There are three built-in message builders.

The DefaultMessageBuilder is best when the schema is very simple, such as when the ByteArrayConverter or StringConverter are being used.

| Value schema    | Value class      | Outgoing message format | JMS message type | Outgoing message body           |
| --------------- | ---------------- | ----------------------- | ---------------- | ------------------------------- |
| null            | Any              | MQFMT_STRING            | TextMessage      | Java Object.toString() of value |
| BYTES           | byte[]           | MQFMT_NONE              | BytesMessage     | Byte array                      |
| STRING          | java.lang.String | MQFMT_STRING            | TextMessage      | String                          |
| Everything else | Any              | MQFMT_STRING            | TextMessage      | Java Object.toString() of value |

If you use the JsonConverter with the DefaultMessageBuilder, the output message will not be JSON; it will be a Java string representation of the value instead. That's why there's a JsonMessageBuilder too which behaves like this:

| Value schema    | Value class      | Outgoing message format | JMS message type | Outgoing message body            |
| --------------- | ---------------- | ----------------------- | ---------------- | -------------------------------- |
| Any             | Any              | MQFMT_STRING            | TextMessage      | JSON representation of the value |

To make the differences clear, here are some examples.

| Input message | Converter       | Value schema      | Message builder       | Output message body | Comment                            |
| ------------- | --------------- | ----------------- | --------------------- | ------------------- | ---------------------------------- |
| ABC           | StringConverter | STRING            | DefaultMessageBuilder | ABC                 | OK                                 |
| ABC           | StringConverter | STRING            | JsonMessageBuilder    | "ABC"               | Quotes added to give a JSON string |
| "ABC"         | JsonConverter   | STRING            | DefaultMessageBuilder | ABC                 | Quotes removed, not a JSON string  |
| "ABC"         | JsonConverter   | STRING            | JsonMessageBuilder    | "ABC"               | OK                                 |
| {"A":"B"}     | JsonConverter   | Compound (STRUCT) | DefaultMessageBuilder | STRUCT{A=B}         | Probably not helpful               |
| {"A":"B"}     | JsonConverter   | Compound (STRUCT) | JsonMessageBuilder    | {"A":"B"}           | OK                                 |

Note that the order of JSON structures is not fixed and fields may be reordered.

To handle the situation in which you already have a Kafka converter that you want to use to build the MQ message payload, the ConverterMessageBuilder is the one to use. Then you would end up using two Converters - one to convert the Kafka message to the internal SinkRecord, and the second to convert that into the MQ message. Since the Converter might also have its own configuration options, you can specify them using a prefix of `mq.message.builder.value.converter`. For example, the following configuration gets the ConverterMessageBuilder to work the same as the JsonMessageBuilder.

```
mq.message.builder=com.ibm.eventstreams.connect.mqsink.builders.ConverterMessageBuilder
mq.message.builder.value.converter=org.apache.kafka.connect.json.JsonConverter
mq.message.builder.value.converter.schemas.enable=false
```

### Key support and partitioning

By default, the connector does not use the keys for the Kafka messages it reads. It can be configured to set the JMS correlation ID using the key of the Kafka records. To configure this behavior, set the `mq.message.builder.key.header` configuration value.

| mq.message.builder.key.header | Key schema | Key class | Recommended value for key.converter                    |
| ----------------------------- |----------- | --------- | ------------------------------------------------------ |
| JMSCorrelationID              | STRING     | String    | org.apache.kafka.connect.storage.StringConverter       |
| JMSCorrelationID              | BYTES      | byte[]    | org.apache.kafka.connect.converters.ByteArrayConverter |

In MQ, the correlation ID is a 24-byte array. As a string, the connector represents it using a sequence of 48 hexadecimal characters. The Kafka key will be truncated to fit into this size.

The connector can be configured to set the Kafka topic, partition and offset as JMS message properties using the `mq.message.builder.*.property` configuration values. If configured, the topic is set as a string property, the partition as an integer property and the offset as a long property. Because these values are set using JMS message properties, they only have an effect if `mq.message.body.jms=true` is set.

## Security

The connector supports authentication with user name and password and also connections secured with TLS using a server-side certificate and mutual authentication with client-side certificates. You can also choose whether to use connection security parameters (MQCSP) depending on the security settings you're using in MQ.

### Setting up TLS using a server-side certificate

To enable use of TLS, set the configuration `mq.ssl.cipher.suite` to the name of the cipher suite which matches the CipherSpec in the SSLCIPH attribute of the MQ server-connection channel. Use the table of supported cipher suites for MQ 9.1 [here](https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_9.1.0/com.ibm.mq.dev.doc/q113220_.htm) as a reference. Note that the names of the CipherSpecs as used in the MQ configuration are not necessarily the same as the cipher suite names that the connector uses. The connector uses the JMS interface so it follows the Java conventions.

You will need to put the public part of the queue manager's certificate in the JSSE truststore used by the Kafka Connect worker that you're using to run the connector. If you need to specify extra arguments to the worker's JVM, you can use the EXTRA_ARGS environment variable.

### Setting up TLS for mutual authentication

You will need to put the public part of the client's certificate in the queue manager's key repository. You will also need to configure the worker's JVM with the location and password for the keystore containing the client's certificate. Alternatively, you can configure a separate keystore and truststore for the connector.

### Troubleshooting

For troubleshooting, or to better understand the handshake performed by the IBM MQ Java client application in combination with your specific JSSE provider, you can enable debugging by setting `javax.net.debug=ssl` in the JVM environment.

## Configuration

The configuration options for the Kafka Connect sink connector for IBM MQ are as follows:

| Name                                    | Description                                                                                               | Type    | Default        | Valid values                      |
| --------------------------------------- | --------------------------------------------------------------------------------------------------------- | ------- | -------------- | --------------------------------- |
| topics or topics.regex                  | List of Kafka source topics                                                                               | string  |                | topic1[,topic2,...]               |
| mq.queue.manager                        | The name of the MQ queue manager                                                                          | string  |                | MQ queue manager name             |
| mq.connection.mode                      | The connection mode - bindings or client                                                                  | string  | client         | client, bindings                  |
| mq.connection.name.list                 | List of connection names for queue manager                                                                | string  |                | host(port)[,host(port),...]       |
| mq.channel.name                         | The name of the server-connection channel                                                                 | string  |                | MQ channel name                   |
| mq.queue                                | The name of the target MQ queue                                                                           | string  |                | MQ queue name                     |
| mq.exactly.once.state.queue             | The name of the MQ queue used to store state when running with exactly-once semantics                     | string  |                | MQ state queue name               |
| mq.user.name                            | The user name for authenticating with the queue manager                                                   | string  |                | User name                         |
| mq.password                             | The password for authenticating with the queue manager                                                    | string  |                | Password                          |
| mq.user.authentication.mqcsp            | Whether to use MQ connection security parameters (MQCSP)                                                  | boolean | true           |                                   |
| mq.ccdt.url                             | The URL for the CCDT file containing MQ connection details                                                | string  |                | URL for obtaining a CCDT file     |
| mq.message.builder                      | The class used to build the MQ message                                                                    | string  |                | Class implementing MessageBuilder |
| mq.message.body.jms                     | Whether to generate the message body as a JMS message type                                                | boolean | false          |                                   |
| mq.time.to.live                         | Time-to-live in milliseconds for messages sent to MQ                                                      | long    | 0 (unlimited)  | [0,...]                           |
| mq.persistent                           | Send persistent or non-persistent messages to MQ                                                          | boolean | true           |                                   |
| mq.ssl.cipher.suite                     | The name of the cipher suite for TLS (SSL) connection                                                     | string  |                | Blank or valid cipher suite       |
| mq.ssl.peer.name                        | The distinguished name pattern of the TLS (SSL) peer                                                      | string  |                | Blank or DN pattern               |
| mq.ssl.keystore.location                | The path to the JKS keystore to use for SSL (TLS) connections                                             | string  | JVM keystore   | Local path to a JKS file          |
| mq.ssl.keystore.password                | The password of the JKS keystore to use for SSL (TLS) connections                                         | string  |                |                                   |
| mq.ssl.truststore.location              | The path to the JKS truststore to use for SSL (TLS) connections                                           | string  | JVM truststore | Local path to a JKS file          |
| mq.ssl.truststore.password              | The password of the JKS truststore to use for SSL (TLS) connections                                       | string  |                |                                   |
| mq.ssl.use.ibm.cipher.mappings          | Whether to set system property to control use of IBM cipher mappings                                      | boolean |                |                                   |
| mq.message.builder.key.header           | The JMS message header to set from the Kafka record key                                                   | string  |                | JMSCorrelationID                  |
| mq.kafka.headers.copy.to.jms.properties | Whether to copy Kafka headers to JMS message properties                                                   | boolean | false          |                                   |
| mq.message.builder.value.converter      | The class and prefix for message builder's value converter                                                | string  |                | Class implementing Converter      |
| mq.message.builder.topic.property       | The JMS message property to set from the Kafka topic                                                      | string  |                | Blank or valid JMS property name  |
| mq.message.builder.partition.property   | The JMS message property to set from the Kafka partition                                                  | string  |                | Blank or valid JMS property name  |
| mq.message.builder.offset.property      | The JMS message property to set from the Kafka offset                                                     | string  |                | Blank or valid JMS property name  |
| mq.reply.queue                          | The name of the reply-to queue                                                                            | string  |                | MQ queue name or queue URI        |
| mq.retry.backoff.ms                     | Wait time, in milliseconds, before retrying after retriable exceptions                                    | long    | 60000          | [0,...]                           |
| mq.message.mqmd.write                   | Whether to enable a custom message builder to write MQ message descriptors                                | boolean | false          |                                   |
| mq.message.mqmd.context                 | Message context to set on the destination queue. This is required when setting some message descriptors.  | string  |                | `IDENTITY`, `ALL`                     |

### Using a CCDT file

Some of the connection details for MQ can be provided in a [CCDT file](https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_9.1.0/com.ibm.mq.con.doc/q016730_.htm) by setting `mq.ccdt.url` in the MQ sink connector configuration file. If using a CCDT file the `mq.connection.name.list` and `mq.channel.name` configuration options are not required.

### Externalizing secrets

[KIP 297](https://cwiki.apache.org/confluence/display/KAFKA/KIP-297%3A+Externalizing+Secrets+for+Connect+Configurations) introduced a mechanism to externalize secrets to be used as configuration for Kafka connectors.

#### Example: externalizing secrets with FileConfigProvider

Given a file `mq-secrets.properties` with the contents:

```shell
secret-key=password
```

Update the worker configuration file to specify the FileConfigProvider which is included by default:

```shell
# Additional properties for the worker configuration to enable use of ConfigProviders
# multiple comma-separated provider types can be specified here
config.providers=file
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
```

Update the connector configuration file to reference `secret-key` in the file:

```shell
mq.password=${file:mq-secret.properties:secret-key}
```

##### Using FileConfigProvider in Kubernetes

To use a file for the `mq.password` in Kubernetes, you create a Secret using the file as described in [the Kubernetes docs](https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-files-from-a-pod).

## Exactly-once message delivery semantics

The MQ sink connector provides at-least-once message delivery by default. This means that each MQ message will be delivered to Kafka, but in failure scenarios it is possible to have duplicated messages delivered to Kafka.

Version 2.0.0 of the MQ sink connector introduced exactly-once message delivery semantics. An additional MQ queue is used to store the state of message deliveries. When exactly-once delivery is enabled MQ messages are delivered to Kafka with no duplicated messages.

### Exactly-once delivery Kafka Connect worker configuration

To enable exactly-once delivery, the MQ sink connector must be run on Kafka Connect version 2.0.0 or later.

**Note**: Exactly-once support for sink connectors is only available in [distributed mode](#running-in-distributed-mode); distributed Connect workers cannot provide exactly-once delivery semantics. Kafka Connect is in distributed mode when [running the connector with Docker](#running-with-docker) and when [deploying the connector to Kubernetes](#deploying-to-kubernetes).

### Exactly-once delivery MQ sink connector configuration

To enable exactly-once delivery, the MQ sink connector must be configured the `mq.exactly.once.state.queue` property set to the name of a pre-configured MQ queue on the same queue manager as the sink MQ queue.

Exactly-once delivery requires that only a single connector task can run in the Kafka Connect instance, hence the `tasks.max` property must be set to `1` to ensure that failure scenarios do not cause duplicated messages to be delivered.

To achieve exactly-once delivery with the MQ sink connector, it is essential to configure its consumer group to ignore records in aborted transactions. You can find detailed instructions in the [Kafka documentation](https://kafka.apache.org/documentation/#connect_exactlyoncesink). Notably, this configuration does not have any additional ACL (Access Control List) requirements.

To start the MQ sink connector with exactly-once delivery, the `config/mq-sink-exactly-once.json` file in this repository can be used as a connector configuration template.

**Note**: Exactly-once delivery requires a clear state queue on start-up otherwise the connector will behave as if it is recovering from a failure state and will attempt to get undelivered messages recorded in the out-of-date state message. Therefore, ensure that the state queue is empty each time exactly-once delivery is enabled (especially if re-enabling the exactly once feature).

### Exactly-once delivery MQ requirements

The following values are recommended across MQ to facilitate the exactly-once behaviour:

- On the channel used for Kafka Connect, `HBINT` should be set to 30 seconds to allow MQ transaction rollbacks to occur more quickly in failure scenarios.
- On the state queue, `DEFSOPT` should be set to `EXCL` to ensure the state queue share option is exclusive.

Exactly-once delivery requires that messages are set to not expire and that all messages on state queue are all persistent (this is to ensure correct behaviour around queue manager restarts).

### Exactly-once failure scenarios

The MQ sink connector is designed to fail on start-up in certain cases to ensure that exactly-once delivery is not compromised.
In some of these failure scenarios, it will be necessary for an MQ administrator to remove messages from the exactly-once state queue before the MQ sink connector can start up and begin to deliver messages from the sink queue again. In these cases, the MQ sink connector will have the `FAILED` status and the Kafka Connect logs will describe any required administrative action.

## Troubleshooting

### Connector in a `FAILED` state

If the connector experiences a non retriable error then a ConnectException will cause the connector to go in to a `FAILED` state. This will require a manual restart using the Kafka Connect REST API to restart the connector.

### Unable to connect to Kafka

You may receive an `org.apache.kafka.common.errors.SslAuthenticationException: SSL handshake failed` error when trying to run the MQ sink connector using SSL to connect to your Kafka cluster. In the case that the error is caused by the following exception: `Caused by: java.security.cert.CertificateException: No subject alternative DNS name matching XXXXX found.`, Java may be replacing the IP address of your cluster with the corresponding hostname in your `/etc/hosts` file. For example, to push Docker images to a custom Docker repository, you may add an entry in this file which corresponds to the IP of your repository e.g. `123.456.78.90    mycluster.icp`. To fix this, you can comment out this line in your `/etc/hosts` file.

### Unsupported cipher suite

When configuring TLS connection to MQ, you may find that the queue manager rejects the cipher suite, in spite of the name looking correct. There are two different naming conventions for cipher suites (<https://www.ibm.com/support/knowledgecenter/SSFKSJ_9.1.0/com.ibm.mq.dev.doc/q113220_.htm>). Setting the configuration option `mq.ssl.use.ibm.cipher.mappings=false` often resolves cipher suite problems.

### `MQRC_NOT_AUTHORIZED` exception

When attempting to send a message to an IBM MQ queue, an MQException with code `MQRC_NOT_AUTHORIZED` (reason code `2035`) and completion code 2 is thrown. This indicates insufficient permissions on the queue and the queue manager.

#### Resolving the problem

1. **Review permissions**: Ensure that the user has necessary permissions for accessing the queue and the queue manager.
2. **Grant authority**: If the user does not have the necessary permissions, assign required authorities to the user.
3. **Set Context**: Set `WMQ_MQMD_MESSAGE_CONTEXT` property for required properties.

    Configure the `mq.message.mqmd.context` property according to the message context. Options include:
      - `ALL`, which corresponds to `WMQ_MDCTX_SET_ALL_CONTEXT`
      - `IDENTITY`, mapped to `WMQ_MDCTX_SET_IDENTITY_CONTEXT`

    **Important:** If your message contains any of the following properties, you must ensure that `WMQ_MQMD_MESSAGE_CONTEXT` is set to either `WMQ_MDCTX_SET_IDENTITY_CONTEXT` or `WMQ_MDCTX_SET_ALL_CONTEXT`:
      - JMS_IBM_MQMD_UserIdentifier
      - JMS_IBM_MQMD_AccountingToken
      - JMS_IBM_MQMD_ApplIdentityData

    Similarly, if your message includes any of the following properties, set the `WMQ_MQMD_MESSAGE_CONTEXT` field to `WMQ_MDCTX_SET_ALL_CONTEXT`:
      - JMS_IBM_MQMD_PutApplType
      - JMS_IBM_MQMD_PutApplName
      - JMS_IBM_MQMD_PutDate
      - JMS_IBM_MQMD_PutTime
      - JMS_IBM_MQMD_ApplOriginData

    Other message properties do not require the `mq.message.mqmd.context` property.

#### Additional tips

- Verify that the length of all properties are correctly set within the allowed limit.
- Do not set the [`JMS_IBM_MQMD_BackoutCount`](https://www.ibm.com/docs/en/ibm-mq/9.3?topic=descriptor-backoutcount-mqlong-mqmd) property.
- Refer to the IBM MQ documentation for detailed configuration guidance:

  - [IBM MQ JMS Message Object Properties](https://www.ibm.com/docs/en/ibm-mq/9.3?topic=application-jms-message-object-properties): This documentation provides details about various properties that can be set on IBM MQ JMS message objects, including their names, types, and descriptions.
  - [IBM MQ Developer Community](https://community.ibm.com/community/user/integration/home): The developer community for IBM MQ, where you can find forums, articles, and resources related to development and troubleshooting for IBM MQ.
  - [IBM MQ troubleshooting guide](https://www.ibm.com/docs/en/ibm-mq/9.3?topic=mq-troubleshooting-support): IBM guide for troubleshooting common issues and errors in IBM MQ.

## Support

A commercially supported version of this connector is available for customers with a support entitlement for [IBM Event Streams](https://www.ibm.com/cloud/event-streams) or [IBM Cloud Pak for Integration](https://www.ibm.com/cloud/cloud-pak-for-integration).

## Issues and contributions

For issues relating specifically to this connector, please use the [GitHub issue tracker](https://github.com/ibm-messaging/kafka-connect-mq-sink/issues). If you do want to submit a Pull Request related to this connector, please read the [contributing guide](CONTRIBUTING.md) first to understand how to sign your commits.

## License

Copyright 2017, 2020, 2023, 2024 IBM Corporation

The IBM MQ sink connector v2 is available under the IBM Event Automation license and IBM Cloud Pak for Integration license. For more information, see the [Event Automation documentation](https://ibm.biz/ea-license).
