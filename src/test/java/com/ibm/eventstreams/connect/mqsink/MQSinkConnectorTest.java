/**
 * Copyright 2017, 2018, 2019, 2023 IBM Corporation
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

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MQSinkConnectorTest {
    @Test
    public void testVersion() {
        final String version = new MQSinkConnector().version();
        final String expectedVersion = System.getProperty("connectorVersion");
        assertEquals("Expected connector version to match version of built jar file.", expectedVersion, version);
    }

    @Test
    public void testConnectorType() {
        final Connector connector = new MQSinkConnector();
        assertTrue(SinkConnector.class.isAssignableFrom(connector.getClass()));
    }

    @Test
    public void testConnectorExactlyOnceSupport() {
        final MQSinkConnector connector = new MQSinkConnector();
        final Map<String, String> props = new HashMap<>();
        props.put("mq.queue.manager", "QM1");
        props.put("mq.connection.mode", "client");
        props.put("mq.connection.name.list", "localhost(1414)");
        props.put("mq.channel.name", "DEV.APP.SVRCONN");
        props.put("mq.queue", "DEV.QUEUE.1");
        props.put("mq.user.authentication.mqcsp", "false");
        props.put("mq.exactly.once.state.queue", "DEV.QUEUE.2");
        connector.start(props);

        // Test with exactly.once.state.queue is set but the max number of tasks is 2
        // (greater than 1)
        assertThrows(ConnectException.class, () -> connector.taskConfigs(2));

        // Test with exactly.once.state.queue is set and the max number of tasks is 1
        List<Map<String, String>> expectedTaskConfigs = new ArrayList<>();
        expectedTaskConfigs.add(props);
        int maxTask = 1;
        assertEquals(connector.taskConfigs(1), expectedTaskConfigs);

        // Test with exactly.once.state.queue is not set
        props.remove("mq.exactly.once.state.queue");
        connector.start(props);
        maxTask = 2;
        expectedTaskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTask; i++) {
            expectedTaskConfigs.add(props);
        }
        assertEquals(connector.taskConfigs(maxTask), expectedTaskConfigs);
    }

    @Test
    public void testConnectorConfigSupportsExactlyOnce() {
        // True if an mq.exactly.once.state.queue value is supplied in the config and
        // 'tasks.max' is 1
        final Map<String, String> configProps = new HashMap<String, String>();
        configProps.put("mq.exactly.once.state.queue", "DEV.QUEUE.2");
        configProps.put("tasks.max", "1");
        assertTrue(MQSinkConnector.configSupportsExactlyOnce(configProps));
        assertTrue(MQSinkConnector
                .configSupportsExactlyOnce(Collections.singletonMap("mq.exactly.once.state.queue", "DEV.QUEUE.2")));
        // False otherwise
        assertFalse(MQSinkConnector.configSupportsExactlyOnce(Collections.singletonMap("tasks.max", "1")));
        assertFalse(MQSinkConnector.configSupportsExactlyOnce(Collections.emptyMap()));
        assertFalse(
                MQSinkConnector.configSupportsExactlyOnce(Collections.singletonMap("mq.exactly.once.state.queue", "")));
        assertFalse(MQSinkConnector
                .configSupportsExactlyOnce(Collections.singletonMap("mq.exactly.once.state.queue", null)));
    }
}
