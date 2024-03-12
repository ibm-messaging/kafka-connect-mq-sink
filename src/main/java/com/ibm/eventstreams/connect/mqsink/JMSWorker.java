/**
 * Copyright 2017, 2020, 2023 IBM Corporation
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
package com.ibm.eventstreams.connect.mqsink;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import javax.jms.DeliveryMode;
import javax.jms.InvalidDestinationRuntimeException;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.eventstreams.connect.mqsink.builders.MessageBuilder;
import com.ibm.eventstreams.connect.mqsink.builders.MessageBuilderFactory;
import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.mq.jms.MQQueue;
import com.ibm.msg.client.wmq.WMQConstants;

/**
 * Writes messages to MQ using JMS. Uses a transacted session, adding messages
 * to the current
 * transaction until told to commit. Automatically reconnects as needed.
 */
public class JMSWorker {
    private static final Logger log = LoggerFactory.getLogger(JMSWorker.class);

    // JMS factory and context
    private MQConnectionFactory mqConnFactory;
    private JMSContext jmsCtxt;
    protected JMSProducer jmsProd;
    protected JMSConsumer jmsCons;

    // MQ objects
    private MQQueue queue;
    protected MQQueue stateQueue;

    // State
    private boolean connected = false; // Whether connected to MQ
    private long reconnectDelayMillis = RECONNECT_DELAY_MILLIS_MIN; // Delay between repeated reconnect attempts

    // Constants
    private int deliveryMode = Message.DEFAULT_DELIVERY_MODE;
    private long timeToLive = Message.DEFAULT_TIME_TO_LIVE;

    final private static long RECONNECT_DELAY_MILLIS_MIN = 64L;
    final private static long RECONNECT_DELAY_MILLIS_MAX = 8192L;

    protected ObjectMapper mapper;

    private boolean isExactlyOnceMode = false;

    private MQConnectionHelper mqConnectionHelper;

    private MessageBuilder messageBuilder;

    public JMSWorker() {
        mapper = new ObjectMapper();
    }

    /**
     * Configure this class.
     *
     * @param config
     * @throws ConnectException
     */
    public void configure(final AbstractConfig config) throws ConnectException {
        log.trace("[{}] Entry {}.configure, props={}", Thread.currentThread().getId(),
                this.getClass().getName());

        isExactlyOnceMode = MQSinkConnector.configSupportsExactlyOnce(config);
        mqConnectionHelper = new MQConnectionHelper(config);

        if (mqConnectionHelper.getUseIBMCipherMappings() != null) {
            System.setProperty("com.ibm.mq.cfg.useIBMCipherMappings", mqConnectionHelper.getUseIBMCipherMappings());
        }

        try {
            mqConnFactory = mqConnectionHelper.createMQConnFactory();
            queue = configureQueue(mqConnectionHelper.getQueueName(), mqConnectionHelper.isMessageBodyJms());
            final Boolean mqmdWriteEnabled = config.getBoolean(MQSinkConfig.CONFIG_NAME_MQ_MQMD_WRITE_ENABLED);
            queue.setBooleanProperty(WMQConstants.WMQ_MQMD_WRITE_ENABLED, mqmdWriteEnabled);

            if (mqmdWriteEnabled) {
                String mqmdMessageContext = config.getString(MQSinkConfig.CONFIG_NAME_MQ_MQMD_MESSAGE_CONTEXT);
                if (mqmdMessageContext != null) {
                    mqmdMessageContext = mqmdMessageContext.toLowerCase(Locale.ENGLISH);
                }
                if ("identity".equals(mqmdMessageContext)) {
                    queue.setIntProperty(WMQConstants.WMQ_MQMD_MESSAGE_CONTEXT,
                            WMQConstants.WMQ_MDCTX_SET_IDENTITY_CONTEXT);
                } else if ("all".equals(mqmdMessageContext)) {
                    queue.setIntProperty(WMQConstants.WMQ_MQMD_MESSAGE_CONTEXT, WMQConstants.WMQ_MDCTX_SET_ALL_CONTEXT);
                }
            }
            if (isExactlyOnceMode) {
                stateQueue = configureQueue(mqConnectionHelper.getStateQueueName(), true);
            }

            this.timeToLive = mqConnectionHelper.getTimeToLive();

            this.deliveryMode = mqConnectionHelper.isPersistent() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;

            this.messageBuilder = MessageBuilderFactory.getMessageBuilder(config);
        } catch (JMSException | JMSRuntimeException | MalformedURLException jmse) {
            log.error("JMS exception {}", jmse);
            throw new JMSWorkerConnectionException("JMS connection failed", jmse);
        }

        log.trace("[{}]  Exit {}.configure", Thread.currentThread().getId(), this.getClass().getName());
    }

    /** Connects to MQ. */
    public void connect() {
        log.trace("[{}] Entry {}.connect", Thread.currentThread().getId(), this.getClass().getName());

        createJMSContext();

        createConsumerForStateQueue();

        configureProducer();

        connected = true;
        log.info("Connection to MQ established");
        log.trace("[{}]  Exit {}.connect", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Internal method to connect to MQ.
     *
     * @throws RetriableException
     *                            Operation failed, but connector should continue to
     *                            retry.
     * @throws ConnectException
     *                            Operation failed and connector should stop.
     */
    private void maybeReconnect() throws ConnectException, RetriableException {
        log.trace("[{}] Entry {}.maybeReconnect", Thread.currentThread().getId(), this.getClass().getName());

        if (connected) {
            log.trace("[{}]  Exit {}.maybeReconnect", Thread.currentThread().getId(), this.getClass().getName());
            return;
        }

        try {
            connect();
            reconnectDelayMillis = RECONNECT_DELAY_MILLIS_MIN;
        } catch (final JMSRuntimeException jmse) {
            // Delay slightly so that repeated reconnect loops don't run too fast
            log.info("Connection to MQ could not be established");
            try {
                Thread.sleep(reconnectDelayMillis);
            } catch (final InterruptedException ie) {
            }

            if (reconnectDelayMillis < RECONNECT_DELAY_MILLIS_MAX) {
                reconnectDelayMillis = reconnectDelayMillis * 2;
            }

            log.error("JMS exception {}", jmse);
            log.trace("[{}]  Exit {}.maybeReconnect, retval=JMSRuntimeException", Thread.currentThread().getId(),
                    this.getClass().getName());
            throw jmse;
        }

        log.trace("[{}]  Exit {}.maybeReconnect, retval=true", Thread.currentThread().getId(),
                this.getClass().getName());
    }

    /**
     * Sends a message to MQ. Adds the message to the current transaction.
     * Reconnects to MQ if required.
     *
     * @param r
     *          The message and schema to send
     *
     * @throws RetriableException
     *                            Operation failed, but connector should continue to
     *                            retry.
     * @throws ConnectException
     *                            Operation failed and connector should stop.
     */
    public void send(final SinkRecord r) throws ConnectException, RetriableException {
        log.trace("[{}] Entry {}.send", Thread.currentThread().getId(), this.getClass().getName());

        maybeReconnect();

        sendSinkRecordToMQ(queue, r);

        log.trace("[{}]  Exit {}.send", Thread.currentThread().getId(), this.getClass().getName());
    }

    /** Closes the connection. */
    public void close() {
        log.trace("[{}] Entry {}.close", Thread.currentThread().getId(), this.getClass().getName());

        try {
            connected = false;

            if (jmsCtxt != null) {
                jmsCtxt.close();
            }
        } catch (final JMSRuntimeException jmse) {
            log.error("", jmse);
        } finally {
            jmsCtxt = null;
            log.debug("Connection to MQ closed");
        }

        log.trace("[{}]  Exit {}.close", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Read a message from the state queue.
     *
     * @return the message
     * @throws JsonProcessingException
     * @throws JMSRuntimeException
     * @throws JMSException
     */
    public Optional<HashMap<String, String>> readFromStateQueue()
            throws JsonProcessingException, JMSRuntimeException, JMSException {
        maybeReconnect();
        if (jmsCons == null) {
            return Optional.empty();
        }
        try {
            final TextMessage message = (TextMessage) jmsCons.receiveNoWait();
            if (message == null) {
                return Optional.empty();
            }
            final HashMap<String, String> stateMap = mapper.readValue(message.getText(),
                    new TypeReference<HashMap<String, String>>() {
                    });
            return Optional.of(stateMap);
        } catch (final JsonProcessingException jpe) {
            log.error("An error occurred while processing (parsing) JSON content from state queue: {}",
                    jpe.getMessage());
            throw jpe;
        } catch (JMSException | JMSRuntimeException jmse) {
            log.error("An error occurred while reading the state queue: {}", jmse.getMessage());
            throw jmse;
        }
    }

    /**
     * Create a queue object. If mbj is true, then create a queue that supports
     * JMS message body.
     *
     * @param queueName
     *                  the name of the queue
     * @param isJms
     *                  whether the queue supports JMS message body
     * @return the queue object
     */
    private MQQueue configureQueue(final String queueName, final Boolean isJms)
            throws JMSException {
        final MQQueue queue = new MQQueue(queueName);
        queue.setMessageBodyStyle(isJms ? WMQConstants.WMQ_MESSAGE_BODY_JMS : WMQConstants.WMQ_MESSAGE_BODY_MQ);
        return queue;
    }

    /**
     * Send the last message to the MQ queue.
     *
     * @param lastCommittedOffsetMap
     * @throws Exception
     *
     * @throws RetriableException
     *                                 Operation failed, but connector should
     *                                 continue to
     *                                 retry.
     * @throws ConnectException
     *                                 Operation failed and connector should stop.
     * @throws JMSException
     * @throws JsonProcessingException
     */
    public void writeLastRecordOffsetToStateQueue(final Map<String, String> lastCommittedOffsetMap)
            throws JsonProcessingException, JMSRuntimeException, JMSException {
        log.trace("[{}] Entry {}.writeLastRecordOffsetToStateQueue", Thread.currentThread().getId(),
                this.getClass().getName());

        maybeReconnect();

        if (lastCommittedOffsetMap == null) {
            log.error("Last committed offset map is null");
            log.trace("[{}]  Exit {}.writeLastRecordOffsetToStateQueue", Thread.currentThread().getId(),
                    this.getClass().getName());
            return;
        }

        final TextMessage message = jmsCtxt.createTextMessage();
        try {
            message.setText(mapper.writeValueAsString(lastCommittedOffsetMap));
            jmsProd.send(stateQueue, message);
        } catch (final JsonProcessingException jpe) {
            log.error("An error occurred while writing to the state queue, Json Processing Exception {}", jpe);
            throw jpe;
        } catch (JMSRuntimeException | JMSException jmse) {
            log.error("An error occurred while writing to the state queue, JMS Exception {}", jmse);
            throw jmse;
        }
        log.trace("[{}]  Exit {}.writeLastRecordOffsetToStateQueue", Thread.currentThread().getId(),
                this.getClass().getName());
    }

    /**
     * Commits the current transaction.
     *
     * @throws RetriableException
     *                            Operation failed, but connector should continue to
     *                            retry.
     * @throws ConnectException
     *                            Operation failed and connector should stop.
     */
    public void commit() {
        log.trace("[{}] Entry {}.commit", Thread.currentThread().getId(), this.getClass().getName());

        maybeReconnect();

        jmsCtxt.commit();
        log.trace("[{}]  Exit {}.commit", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Builds the JMS message and sends it to MQ.
     *
     * @param queue
     *               The MQ queue to send the message to
     * @param record
     *               The message and schema to send
     * @throws JMSException
     */
    private void sendSinkRecordToMQ(final MQQueue queue, final SinkRecord record) {
        maybeReconnect();
        final Message m = messageBuilder.fromSinkRecord(jmsCtxt, record);
        jmsProd.send(queue, m);
    }

    protected void createJMSContext() {
        if (mqConnectionHelper.getUserName() != null) {
            jmsCtxt = mqConnFactory.createContext(mqConnectionHelper.getUserName(), mqConnectionHelper.getPassword().value(),
                    JMSContext.SESSION_TRANSACTED);
        } else {
            jmsCtxt = mqConnFactory.createContext(JMSContext.SESSION_TRANSACTED);
        }
    }

    protected void configureProducer() {
        jmsProd = jmsCtxt.createProducer();
        jmsProd.setDeliveryMode(deliveryMode);
        jmsProd.setTimeToLive(timeToLive);
    }

    protected void createConsumerForStateQueue() {
        if (stateQueue != null) {
            try {
                jmsCons = jmsCtxt.createConsumer(stateQueue);
            } catch (final InvalidDestinationRuntimeException e) {
                log.error("An invalid state queue is specified.", e);
                throw e;
            }
        }
    }
}
