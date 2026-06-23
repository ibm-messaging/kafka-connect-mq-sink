/**
 * Copyright 2026 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.ibm.eventstreams.connect.mqsink;

import org.junit.ClassRule;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Extends the MQ Testcontainers harness with a Kafka broker for full connector round-trip ITs.
 *
 * <p>Uses {@link KafkaContainer} with the official {@code apache/kafka} image (KRaft mode, no ZooKeeper).
 */
public abstract class AbstractKafkaMqRoundTripIT extends AbstractJMSContextIT {

    protected static final String ROUND_TRIP_TOPIC = "mq.headers.roundtrip";
    protected static final String SOURCE_QUEUE = DEFAULT_SINK_QUEUE_NAME;
    protected static final String SINK_QUEUE = DEFAULT_SINK_STATE_QUEUE_NAME;

    /** Official Apache Kafka JVM image; KRaft is the default for this Testcontainers implementation. */
    private static final String KAFKA_IMAGE = "apache/kafka:4.2.1";

    @ClassRule
    public static final KafkaContainer KAFKA = new KafkaContainer(KAFKA_IMAGE);

    protected String kafkaBootstrapServers() {
        return KAFKA.getBootstrapServers();
    }
}
