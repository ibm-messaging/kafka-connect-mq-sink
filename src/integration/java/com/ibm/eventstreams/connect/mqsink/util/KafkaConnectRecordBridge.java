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
package com.ibm.eventstreams.connect.mqsink.util;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Writes {@link SourceRecord} instances to a real Kafka topic and reads them back as {@link SinkRecord}
 * instances, preserving Connect header schemas and values.
 */
public final class KafkaConnectRecordBridge implements AutoCloseable {

    private static final String HEADER_SCHEMA_PREFIX = "__connect.header.schema.";
    private static final String KEY_SCHEMA_HEADER = "__connect.key.schema";
    private static final String VALUE_SCHEMA_HEADER = "__connect.value.schema";
    private static final String NULL_SENTINEL = "__connect.null";

    private final KafkaProducer<byte[], byte[]> producer;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final String topic;

    public KafkaConnectRecordBridge(final String bootstrapServers, final String topic) {
        this.topic = topic;
        this.producer = new KafkaProducer<>(producerProps(bootstrapServers));
        this.consumer = new KafkaConsumer<>(consumerProps(bootstrapServers));
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public void produce(final SourceRecord sourceRecord) {
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                topic,
                0,
                sourceRecord.timestamp(),
                encodeKey(sourceRecord),
                encodeValue(sourceRecord),
                toKafkaHeaders(sourceRecord));
        producer.send(record);
        producer.flush();
    }

    public SinkRecord consumeSinkRecord(final Duration timeout) {
        final long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
            for (final ConsumerRecord<byte[], byte[]> record : records) {
                if (topic.equals(record.topic())) {
                    return toSinkRecord(record);
                }
            }
        }
        throw new IllegalStateException("No Kafka record consumed from topic " + topic + " within " + timeout);
    }

    @Override
    public void close() {
        producer.close();
        consumer.close();
    }

    private static Properties producerProps(final String bootstrapServers) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }

    private static Properties consumerProps(final String bootstrapServers) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "mq-sink-it-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return props;
    }

    private static Headers toKafkaHeaders(final SourceRecord sourceRecord) {
        final RecordHeaders kafkaHeaders = new RecordHeaders();
        if (sourceRecord.keySchema() != null) {
            kafkaHeaders.add(KEY_SCHEMA_HEADER, sourceRecord.keySchema().type().name().getBytes(StandardCharsets.UTF_8));
        }
        if (sourceRecord.valueSchema() != null) {
            kafkaHeaders.add(VALUE_SCHEMA_HEADER, sourceRecord.valueSchema().type().name().getBytes(StandardCharsets.UTF_8));
        }
        for (final Header header : sourceRecord.headers()) {
            kafkaHeaders.add(header.key(), encodeHeaderValue(header));
            if (header.schema() != null) {
                kafkaHeaders.add(HEADER_SCHEMA_PREFIX + header.key(),
                        header.schema().type().name().getBytes(StandardCharsets.UTF_8));
            }
        }
        return kafkaHeaders;
    }

    private static byte[] encodeKey(final SourceRecord sourceRecord) {
        if (sourceRecord.key() == null) {
            return null;
        }
        return encodeObject(sourceRecord.keySchema(), sourceRecord.key());
    }

    private static byte[] encodeValue(final SourceRecord sourceRecord) {
        if (sourceRecord.value() == null) {
            return null;
        }
        return encodeObject(sourceRecord.valueSchema(), sourceRecord.value());
    }

    private static byte[] encodeHeaderValue(final Header header) {
        if (header.value() == null) {
            return NULL_SENTINEL.getBytes(StandardCharsets.UTF_8);
        }
        return encodeObject(header.schema(), header.value());
    }

    private static byte[] encodeObject(final Schema schema, final Object value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static SinkRecord toSinkRecord(final ConsumerRecord<byte[], byte[]> record) {
        final Schema keySchema = schemaFromKafkaHeader(record.headers(), KEY_SCHEMA_HEADER);
        final Schema valueSchema = schemaFromKafkaHeader(record.headers(), VALUE_SCHEMA_HEADER);
        final ConnectHeaders connectHeaders = decodeConnectHeaders(record.headers());

        return new SinkRecord(
                record.topic(),
                record.partition(),
                keySchema,
                decodeObject(keySchema, record.key()),
                valueSchema,
                decodeObject(valueSchema, record.value()),
                record.offset(),
                record.timestamp(),
                TimestampType.CREATE_TIME,
                connectHeaders);
    }

    private static ConnectHeaders decodeConnectHeaders(final org.apache.kafka.common.header.Headers kafkaHeaders) {
        final ConnectHeaders connectHeaders = new ConnectHeaders();
        for (final org.apache.kafka.common.header.Header kafkaHeader : kafkaHeaders) {
            final String key = kafkaHeader.key();
            if (key.startsWith("__connect.")) {
                continue;
            }
            final Schema schema = schemaFromKafkaHeader(kafkaHeaders, HEADER_SCHEMA_PREFIX + key);
            final Object value = decodeHeaderValue(schema, kafkaHeader.value());
            addConnectHeader(connectHeaders, key, schema, value);
        }
        return connectHeaders;
    }

    private static void addConnectHeader(final ConnectHeaders headers, final String key, final Schema schema,
            final Object value) {
        if (value == null) {
            headers.addString(key, null);
            return;
        }
        if (schema != null && schema.type() == Schema.Type.BYTES) {
            headers.addBytes(key, (byte[]) value);
            return;
        }
        headers.addString(key, value.toString());
    }

    private static Object decodeHeaderValue(final Schema schema, final byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        if (ArraysEqual(bytes, NULL_SENTINEL.getBytes(StandardCharsets.UTF_8))) {
            return null;
        }
        return decodeObject(schema, bytes);
    }

    private static Object decodeObject(final Schema schema, final byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        if (schema != null && schema.type() == Schema.Type.BYTES) {
            return bytes;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static Schema schemaFromKafkaHeader(final org.apache.kafka.common.header.Headers kafkaHeaders,
            final String headerName) {
        final org.apache.kafka.common.header.Header header = kafkaHeaders.lastHeader(headerName);
        if (header == null) {
            return null;
        }
        final String typeName = new String(header.value(), StandardCharsets.UTF_8);
        return schemaForType(Schema.Type.valueOf(typeName));
    }

    private static Schema schemaForType(final Schema.Type type) {
        switch (type) {
            case INT8:
                return Schema.INT8_SCHEMA;
            case INT16:
                return Schema.INT16_SCHEMA;
            case INT32:
                return Schema.INT32_SCHEMA;
            case INT64:
                return Schema.INT64_SCHEMA;
            case FLOAT32:
                return Schema.FLOAT32_SCHEMA;
            case FLOAT64:
                return Schema.FLOAT64_SCHEMA;
            case BOOLEAN:
                return Schema.BOOLEAN_SCHEMA;
            case STRING:
                return Schema.STRING_SCHEMA;
            case BYTES:
                return Schema.BYTES_SCHEMA;
            default:
                return null;
        }
    }

    private static boolean ArraysEqual(final byte[] left, final byte[] right) {
        if (left.length != right.length) {
            return false;
        }
        for (int i = 0; i < left.length; i++) {
            if (left[i] != right[i]) {
                return false;
            }
        }
        return true;
    }
}
