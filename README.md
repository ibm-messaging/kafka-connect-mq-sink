# Kafka Connect sink connector for IBM MQ

kafka-connect-mqsink is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect) for copying data from Apache Kafka into IBM MQ.

The connector is supplied as source code which you can easily build into a JAR file.


## Building the connector
To build the connector, you must have the following installed:
* [git](https://git-scm.com/)
* [Maven](https://maven.apache.org)
* Java 7 or later

Download the MQ client JAR by following the instructions in [Getting the IBM MQ classes for Java and JMS](https://www-01.ibm.com/support/docview.wss?uid=swg21683398). Once you've accepted the license, download the *IBM MQ JMS and Java redistributable client* file (currently called `9.0.0.1-IBM-MQC-Redist-Java.zip`). Unpack the ZIP file.

Clone the repository with the following command:
```shell
git clone https://github.com/ibm-messaging/kafka-connect-mq-sink.git
```

Change directory into the `kafka-connect-mq-sink` directory:
```shell
cd kafka-connect-mq-sink
```

Copy the JAR file `allclient-9.0.0.1.jar` that you unpacked from the ZIP file earlier into the `kafka-connect-mq-sink` directory.

Run the following command to create a local Maven repository containing just this file so that it can be used to build the connector:
```shell
mvn deploy:deploy-file -Durl=file://local-maven-repo -Dfile=allclient-9.0.0.1.jar -DgroupId=com.ibm.mq -DartifactId=allclient -Dpackaging=jar -Dversion=9.0.0.1
```

Build the connector using Maven:
```shell
mvn clean package
```

Once built, the output is a single JAR `target/kafka-connect-mq-sink-0.1-SNAPSHOT-jar-with-dependencies.jar` which contains all of the required dependencies.


## Running the connector
To run the connector, you must have:
* The JAR from building the connector
* A properties file containing the configuration for the connector
* Apache Kafka
* IBM MQ v8.0 or later

The connector can be run in a Kafka Connect worker in either standalone (single process) or distributed mode. It's a good idea to start in standalone mode.

You need two configuration files, one for the configuration that applies to all of the connectors such as the Kafka bootstrap servers, and another for the configuration specific to the MQ sink connector such as the connection information for your queue manager. For the former, the Kafka distribution includes a file called `connect-standalone.properties` that you can use as a starting point. For the latter, you can use `config/mq-sink.properties` in this repository.

The connector connects to MQ using a client connection. You must provide the name of the queue manager, the connection name (one or more host/port pairs) and the channel name. In addition, you can provide a user name and password if the queue manager is configured to require them for client connections. If you look at the supplied `config/mq-sink.properties`, you'll see how to specify the configuration required.

To run the connector in standalone mode from the directory into which you installed Apache Kafka, you use a command like this:

``` shell
bin/connect-standalone.sh connect-standalone.properties mq-sink.properties
```


## Data formats
Kafka Connect is very flexible but it's important to understand the way that it processes messages to end up with a reliable system. When the connector encounters a message that it cannot process, it stops rather than throwing the message away. Therefore, you need to make sure that the configuration you use can handle the messages the connector will process.

Each message in Kafka Connect is associated with a representation of the message format known as a *schema*. Each Kafka message actually has two parts, key and value, and each part has its own schema. The MQ sink connector does not currently use message keys, but some of the configuration options use the word *Value* because they refer to the Kafka message value.

When the MQ sink connector reads a message from Kafka, it is processed using a *converter* which chooses a schema to represent the message format and creates a Java object containing the message value. The MQ sink connector then converts this internal format into the message it sends to MQ.

There's no single configuration that will always be right, but here are some high-level suggestions.

* Message values are treated as byte arrays, pass byte array into MQ message with format MQFMT_NONE
```
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
```
* Message values are treated as strings, pass string into MQ message with format MQFMT_STRING
```
value.converter=org.apache.kafka.connect.storage.StringConverter
```
* Message values are treated as byte arrays, pass byte array into JMS BytesMessage with MQRFH2 header
```
mq.message.body.jms=true
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
```
* Message values are treated as strings, pass string into JMS TextMessage with MQRFH2 header
```
mq.message.body.jms=true
value.converter=org.apache.kafka.connect.storage.StringConverter
```

### The gory detail
The messages received from Kafka are processed by a converter which chooses a schema to represent the message format and creates a Java object containing the message value. There are three basic converters built into Apache Kafka.

| Converter class                                        | Value schema        | Value class        |
| ------------------------------------------------------ | ------------------- | ------------------ |
| org.apache.kafka.connect.converters.ByteArrayConverter | OPTIONAL_BYTES      | byte[]             |
| org.apache.kafka.connect.storage.StringConverter       | OPTIONAL_STRING     | java.lang.String   |
| org.apache.kafka.connect.json.JsonConverter            | Depends on message  | Depends on message |

The MQ sink connector has a configuration option *mq.message.body.jms* that controls whether it generates the MQ messages as JMS message types. By default, *mq.message.body.jms=false* which gives the following behaviour.

| Value schema    | Value class      | Outgoing message format | Outgoing message body           |
| --------------- | ---------------- | ----------------------- | ------------------------------- |
| null            | Any              | MQFMT_STRING            | Java Object.toString() of value |
| OPTIONAL_BYTES  | byte[]           | MQFMT_NONE              | Byte array                      |
| OPTIONAL_STRING | java.lang.String | MQFMT_STRING            | String                          |
| Other primitive | Any              | *EXCEPTION*             | *EXCEPTION*                     |
| Compound        | Any              | MQFMT_STRING            | Java Object.toString() of value |

When you set *mq.message.body.jms=true*, the MQ messages are generated as JMS messages. This is appropriate if the applications receiving the messages are themselves using JMS. This gives the following behaviour.

| Value schema    | Value class      | Outgoing message format | Outgoing message body  |
| --------------- | ---------------- | ----------------------- | ---------------------- |
| null            | Any              | JMS TextMessage         | Java toString of value |
| OPTIONAL_BYTES  | byte[]           | JMS BytesMessage        | Byte array             |
| OPTIONAL_STRING | java.lang.String | JMS TextMessage         | String                 |
| Other primitive | Any              | *EXCEPTION*             | *EXCEPTION*            |
| Compound        | Any              | JMS TextMessage         | Java toString of value |

There are three basic converters built into Apache Kafka, with the likely useful combinations in **bold**.

| Converter class                                        | MQ message                  |
| ------------------------------------------------------ | --------------------------- |
| org.apache.kafka.connect.converters.ByteArrayConverter | **Binary data**             |
| org.apache.kafka.connect.storage.StringConverter       | **String data**             |
| org.apache.kafka.connect.json.JsonConverter            | String data, but not useful |

In addition, there is another converter for the Avro format that is part of the Confluent Platform. This has not been tested with the MQ sink connector at this time.


## Configuration
The configuration options for the MQ Sink Connector are as follows:

| Name                    | Description                                                | Type    | Default       | Valid values                |
| ----------------------- | ---------------------------------------------------------- | ------- | ------------- | --------------------------- |
| topics                  | List of Kafka source topics                                | string  |               | topic1[,topic2,...]         |
| mq.queue.manager        | The name of the MQ queue manager                           | string  |               | MQ queue manager name       |
| mq.connection.name.list | List of connection names for queue manager                 | string  |               | host(port)[,host(port),...] |
| mq.channel.name         | The name of the server-connection channel                  | string  |               | MQ channel name             |
| mq.queue                | The name of the target MQ queue                            | string  |               | MQ queue name               |
| mq.user.name            | The user name for authenticating with the queue manager    | string  |               | User name                   |
| mq.password             | The password for authenticating with the queue manager     | string  |               | Password                    |
| mq.message.body.jms     | Whether to generate the message body as a JMS message type | boolean | false         |                             |
| mq.time.to.live         | Time-to-live in milliseconds for messages sent to MQ       | long    | 0 (unlimited) | [0,...]                     |
| mq.persistent           | Send persistent or non-persistent messages to MQ           | boolean | true          |                             |


## Future enhancements
The first version of the connector is intentionally basic. The idea is to enhance it with additional features to make it more capable. Some possible future enhancements are:
* TLS connections
* Message key support
* JMX metrics
* Improved JSON support
* Testing with the Confluent Platform Avro converter and Schema Registry


## License
Copyright 2017 IBM Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    (http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.The project is licensed under the Apache 2 license.