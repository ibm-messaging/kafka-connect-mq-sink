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

import java.util.HashMap;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class MessageBuilderFactoryTest {

    final Map<String, String> emptyProps = new HashMap<>();

    @Test
    public void testGetMessageBuilder_ForJsonMessageBuilder() {
        final MessageBuilder messageBuilder = MessageBuilderFactory
                .getMessageBuilder("com.ibm.eventstreams.connect.mqsink.builders.JsonMessageBuilder", emptyProps);
        Assertions.assertThat(messageBuilder).isInstanceOf(JsonMessageBuilder.class);
    }

    @Test
    public void testGetMessageBuilder_ForDefaultMessageBuilder() {
        final MessageBuilder messageBuilder = MessageBuilderFactory
                .getMessageBuilder("com.ibm.eventstreams.connect.mqsink.builders.DefaultMessageBuilder", emptyProps);
        Assertions.assertThat(messageBuilder).isInstanceOf(DefaultMessageBuilder.class);
    }

    @Test(expected = MessageBuilderException.class)
    public void testGetMessageBuilder_JunkClass() {
        MessageBuilderFactory.getMessageBuilder("casjsajhasdhusdo;iasd", emptyProps);
    }

    @Test(expected = MessageBuilderException.class)
    public void testGetMessageBuilder_NullProps() {
        MessageBuilderFactory.getMessageBuilder("casjsajhasdhusdo;iasd", null);
    }
}