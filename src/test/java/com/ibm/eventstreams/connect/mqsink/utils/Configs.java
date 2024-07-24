/**
 * Copyright 2024 IBM Corporation
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.eventstreams.connect.mqsink.utils;

import java.util.Map;
import java.util.HashMap;

import org.apache.kafka.common.config.AbstractConfig;

import com.ibm.eventstreams.connect.mqsink.MQSinkConfig;

import static com.ibm.eventstreams.connect.mqsink.AbstractJMSContextIT.DEFAULT_MESSAGE_BUILDER;

public class Configs {

    static Map<String, String> options = new HashMap<>();

    static {
        options.put(MQSinkConfig.CONFIG_NAME_MQ_QUEUE, "TEST.QUEUE");
        options.put(MQSinkConfig.CONFIG_NAME_MQ_QUEUE_MANAGER, "TEST.QUEUE.MANAGER");
        options.put(MQSinkConfig.CONFIG_NAME_MQ_MESSAGE_BUILDER, DEFAULT_MESSAGE_BUILDER);
    }

    public static AbstractConfig defaultConfig() {
        return new AbstractConfig(MQSinkConfig.config(), options);
    }

    public static AbstractConfig customConfig(Map<String, String> overrides) {
        Map<String, String> customOptions = new HashMap<>(options);
        customOptions.putAll(overrides);
        return new AbstractConfig(MQSinkConfig.config(), customOptions);
    }
}