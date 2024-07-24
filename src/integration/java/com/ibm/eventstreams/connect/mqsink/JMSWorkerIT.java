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
package com.ibm.eventstreams.connect.mqsink;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.HashMap;
import java.util.Optional;

import com.ibm.eventstreams.connect.mqsink.utils.Configs;

import org.junit.After;
import org.junit.Test;

public class JMSWorkerIT extends AbstractJMSContextIT {

    @After
    public void after() throws Exception {
        clearAllMessages(DEFAULT_SINK_QUEUE_NAME);
        clearAllMessages(DEFAULT_SINK_STATE_QUEUE_NAME);
    }

    @Test
    public void testWriteLastRecordOffsetToStateQueue() throws Exception {
        final Map<String, String> connectorProps = getExactlyOnceConnectionDetails();

        final JMSWorker jmsWorker = new JMSWorker();
        jmsWorker.configure(Configs.customConfig(connectorProps));

        assertThat(jmsWorker.readFromStateQueue()).isEqualTo(Optional.empty());

        final HashMap<String, String> lastCommittedOffsetMap = new HashMap<String, String>();
        final String key = TOPIC + "-" + PARTITION;
        lastCommittedOffsetMap.put(key, "124");
        jmsWorker.writeLastRecordOffsetToStateQueue(lastCommittedOffsetMap);
        assertThat(jmsWorker.readFromStateQueue()).isEqualTo(Optional.of(new HashMap<String, String>() {
            {
                put(key, "124");
            }
        }));
        // As the last state offset is already called it gets removed from the queue
        assertThat(jmsWorker.readFromStateQueue()).isEqualTo(Optional.empty());

        jmsWorker.writeLastRecordOffsetToStateQueue(null);
        assertThat(jmsWorker.readFromStateQueue()).isEqualTo(Optional.empty());
    }
}
