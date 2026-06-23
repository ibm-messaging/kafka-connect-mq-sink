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

import org.testcontainers.containers.GenericContainer;

/**
 * Grants IBM MQ authorities needed for JMS puts with MQMD write and all-context (as used by
 * {@link JmsMqPutHelper}).
 */
public final class MqContainerAuthHelper {

    private MqContainerAuthHelper() {
    }

    /**
     * Grants {@code +setall} on the queue and queue manager for the test application user.
     *
     * <p>Required when putting with {@code WMQ_MQMD_WRITE_ENABLED} and
     * {@code WMQ_MDCTX_SET_ALL_CONTEXT}; without this, IBM MQ returns {@code MQRC_NOT_AUTHORIZED}
     * (2035).
     */
    public static void grantAppUserMqmdPut(final GenericContainer<?> mqContainer, final String queueManager,
            final String queueName, final String username) throws Exception {
        mqContainer.execInContainer("setmqaut",
                "-m", queueManager,
                "-n", queueName,
                "-p", username,
                "-t", "queue",
                "+setall", "+get", "+browse", "+put", "+inq");

        mqContainer.execInContainer("setmqaut",
                "-m", queueManager,
                "-p", username,
                "-t", "qmgr",
                "+setall");
    }
}
