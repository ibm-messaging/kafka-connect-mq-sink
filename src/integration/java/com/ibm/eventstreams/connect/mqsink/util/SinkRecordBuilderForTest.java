/**
 * Copyright 2023, 2024 IBM Corporation
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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

public class SinkRecordBuilderForTest {
    private String topic;
    private Integer partition;
    private Schema keySchema;
    private Object key;
    private Schema valueSchema;
    private Object value;
    private Long offset;

    public SinkRecordBuilderForTest() {
    }

    public SinkRecordBuilderForTest topic(final String topic) {
        this.topic = topic;
        return this;
    }

    public SinkRecordBuilderForTest partition(final Integer partition) {
        this.partition = partition;
        return this;
    }

    public SinkRecordBuilderForTest keySchema(final Schema keySchema) {
        this.keySchema = keySchema;
        return this;
    }

    public SinkRecordBuilderForTest key(final Object key) {
        this.key = key;
        return this;
    }

    public SinkRecordBuilderForTest valueSchema(final Schema valueSchema) {
        this.valueSchema = valueSchema;
        return this;
    }

    public SinkRecordBuilderForTest value(final Object value) {
        this.value = value;
        return this;
    }

    public SinkRecordBuilderForTest offset(final Long offset) {
        this.offset = offset;
        return this;
    }

    public SinkRecord build() {
        return new SinkRecord(this.topic, this.partition, this.keySchema, this.key, this.valueSchema, this.value,
                this.offset);
    }

    public String toString() {
        return "SinkRecord.SinkRecordBuilder(topic=" + this.topic + ", partition=" + this.partition + ", keySchema="
                + this.keySchema + ", key=" + this.key + ", valueSchema=" + this.valueSchema + ", value=" + this.value
                + ")";
    }
}
