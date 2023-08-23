/**
 * Copyright 2022, 2023 IBM Corporation
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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsink.AbstractJMSContextIT;

public class JsonMessageBuilderIT extends AbstractJMSContextIT {

    private MessageBuilder builder;

    @Before
    public void prepareMessageBuilder() {
        builder = new JsonMessageBuilder();
    }

    private SinkRecord generateSinkRecord(final Schema valueSchema, final Object value) {
        final String topic = "TOPIC.NAME";
        final int partition = 0;
        final long offset = 0;
        final Schema keySchema = Schema.STRING_SCHEMA;
        final String key = "mykey";

        return new SinkRecord(topic, partition,
                keySchema, key,
                valueSchema, value,
                offset);
    }

    @Test
    public void buildTextMessageWithoutSchema() throws Exception {
        createAndVerifyStringMessage(null, "Hello World");
    }

    @Test
    public void buildTextMessageWithSchema() throws Exception {
        createAndVerifyStringMessage(Schema.STRING_SCHEMA, "Hello World with a schema");
    }

    @Test
    public void buildStructMessage() throws Exception {
        final Struct testObject = generateComplexObjectAsStruct();
        final Schema testSchema = testObject.schema();

        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(testSchema, testObject));
        final String contents = message.getBody(String.class);

        final JSONObject jsonContents = new JSONObject(contents);
        assertEquals(3, jsonContents.length());
        assertEquals("this is a string", jsonContents.getString("mystring"));
        assertEquals(true, jsonContents.getJSONObject("myobj").getBoolean("mybool"));
        assertEquals(12345, jsonContents.getJSONObject("myobj").getInt("myint"));
        assertEquals(12.4, jsonContents.getJSONObject("myobj").getDouble("myfloat"), 0.0001);
        assertEquals(4, jsonContents.getJSONArray("myarray").length());
        assertEquals("first", jsonContents.getJSONArray("myarray").getString(0));
    }

    @Test
    public void buildMapMessage() throws Exception {
        final Object testObject = generateComplexObjectAsMap();

        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(null, testObject));
        final String contents = message.getBody(String.class);

        final JSONObject jsonContents = new JSONObject(contents);
        assertEquals(3, jsonContents.length());
        assertEquals("this is a string", jsonContents.getString("mystring"));
        assertEquals(true, jsonContents.getJSONObject("myobj").getBoolean("mybool"));
        assertEquals(12345, jsonContents.getJSONObject("myobj").getInt("myint"));
        assertEquals(12.4, jsonContents.getJSONObject("myobj").getDouble("myfloat"), 0.0001);
        assertEquals(4, jsonContents.getJSONArray("myarray").length());
        assertEquals("first", jsonContents.getJSONArray("myarray").getString(0));
    }

    private Struct generateComplexObjectAsStruct() {
        final Schema innerSchema = SchemaBuilder.struct()
                .name("com.ibm.eventstreams.tests.Inner")
                .field("mybool", Schema.BOOLEAN_SCHEMA)
                .field("myint", Schema.INT32_SCHEMA)
                .field("myfloat", Schema.FLOAT32_SCHEMA)
                .field("mybytes", Schema.BYTES_SCHEMA)
                .build();

        final Schema complexSchema = SchemaBuilder.struct()
                .name("com.ibm.eventstreams.tests.Complex")
                .field("mystring", Schema.STRING_SCHEMA)
                .field("myobj", innerSchema)
                .field("myarray", SchemaBuilder.array(Schema.STRING_SCHEMA))
                .build();

        final List<String> innerary = new ArrayList<>();
        innerary.add("first");
        innerary.add("second");
        innerary.add("third");
        innerary.add("fourth");

        final Struct obj = new Struct(complexSchema)
                .put("mystring", "this is a string")
                .put("myobj",
                        new Struct(innerSchema)
                                .put("mybool", true)
                                .put("myint", 12345)
                                .put("myfloat", 12.4f)
                                .put("mybytes", "Hello".getBytes()))
                .put("myarray", innerary);

        return obj;
    }

    private Map<String, Object> generateComplexObjectAsMap() {
        final Map<String, Object> obj = new HashMap<>();

        obj.put("mystring", "this is a string");

        final Map<String, Object> innerobj = new HashMap<>();
        innerobj.put("mybool", true);
        innerobj.put("myint", 12345);
        innerobj.put("myfloat", 12.4f);
        innerobj.put("mybytes", "Hello".getBytes());
        obj.put("myobj", innerobj);

        final List<String> innerary = new ArrayList<>();
        innerary.add("first");
        innerary.add("second");
        innerary.add("third");
        innerary.add("fourth");
        obj.put("myarray", innerary);

        return obj;
    }

    private void createAndVerifyStringMessage(final Schema valueSchema, final String value) throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(valueSchema, value));
        assertEquals("\"" + value + "\"", message.getBody(String.class));

        final TextMessage textmessage = (TextMessage) message;
        assertEquals("\"" + value + "\"", textmessage.getText());
    }
}
