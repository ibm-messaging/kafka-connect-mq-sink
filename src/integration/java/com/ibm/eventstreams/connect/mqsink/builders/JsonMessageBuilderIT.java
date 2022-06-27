package com.ibm.eventstreams.connect.mqsink.builders;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.connect.data.Schema;
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

    private SinkRecord generateSinkRecord(Schema valueSchema, Object value) {
        final String TOPIC = "TOPIC.NAME";
        final int PARTITION = 0;
        final long OFFSET = 0;
        final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
        final String KEY = "mykey";

        return new SinkRecord(TOPIC, PARTITION,
                              KEY_SCHEMA, KEY,
                              valueSchema, value,
                              OFFSET);
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
    public void buildJsonMessageWithoutSchema() throws Exception {
        Object testObject = generateComplexObject();

        Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(null, testObject));
        String contents = message.getBody(String.class);

        JSONObject jsonContents = new JSONObject(contents);
        assertEquals(3, jsonContents.length());
        assertEquals("this is a string", jsonContents.getString("mystring"));
        assertEquals(true, jsonContents.getJSONObject("myobj").getBoolean("mybool"));
        assertEquals(12345, jsonContents.getJSONObject("myobj").getInt("myint"));
        assertEquals(12.4, jsonContents.getJSONObject("myobj").getDouble("myfloat"), 0.0001);
        assertEquals(4, jsonContents.getJSONArray("myarray").length());
        assertEquals("first", jsonContents.getJSONArray("myarray").getString(0));
    }


    private Object generateComplexObject() {
        Map<String, Object> obj = new HashMap<>();

        obj.put("mystring", "this is a string");

        Map<String, Object> innerobj = new HashMap<>();
        innerobj.put("mybool", true);
        innerobj.put("myint", 12345);
        innerobj.put("myfloat", 12.4f);
        innerobj.put("mybytes", "Hello".getBytes());
        obj.put("myobj", innerobj);

        List<String> innerary = new ArrayList<>();
        innerary.add("first");
        innerary.add("second");
        innerary.add("third");
        innerary.add("fourth");
        obj.put("myarray", innerary);

        return obj;
    }


    private void createAndVerifyStringMessage(Schema valueSchema, String value) throws Exception {
        Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(valueSchema, value));
        assertEquals("\"" + value + "\"", message.getBody(String.class));

        TextMessage textmessage = (TextMessage) message;
        assertEquals("\"" + value + "\"", textmessage.getText());
    }
}
