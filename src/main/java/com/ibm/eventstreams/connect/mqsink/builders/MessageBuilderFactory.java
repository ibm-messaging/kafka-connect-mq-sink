/**
 * Copyright 2023 IBM Corporation
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
package com.ibm.eventstreams.connect.mqsink.builders;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.eventstreams.connect.mqsink.MQSinkConfig;


public class MessageBuilderFactory {

    private static final Logger log = LoggerFactory.getLogger(MessageBuilderFactory.class);

    public static MessageBuilder getMessageBuilder(final AbstractConfig config) throws ConnectException {
        return getMessageBuilder(
            config.getString(MQSinkConfig.CONFIG_NAME_MQ_MESSAGE_BUILDER),
                config);
    }

    protected static MessageBuilder getMessageBuilder(final String builderClass, final AbstractConfig config)
            throws ConnectException {

        final MessageBuilder builder;

        try {
            final Class<? extends MessageBuilder> c = Class.forName(builderClass).asSubclass(MessageBuilder.class);
            builder = c.newInstance();
            builder.configure(config.originalsStrings());
        } catch (ClassNotFoundException | ClassCastException | IllegalAccessException | InstantiationException
                | NullPointerException exc) {
            log.error("Could not instantiate message builder {}", builderClass);
            throw new MessageBuilderException("Could not instantiate message builder", exc);
        }

        return builder;
    }
}
