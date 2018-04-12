/**
 * Copyright 2017, 2018 IBM Corporation
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
package com.ibm.mq.kafkaconnect;

import com.ibm.mq.MQException;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.jms.*;
import com.ibm.mq.kafkaconnect.builders.MessageBuilder;
import com.ibm.msg.client.wmq.WMQConstants;

import java.util.Map;

import javax.jms.DeliveryMode;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Writes messages to MQ using JMS. Uses a transacted session, adding messages to the current
 * transaction until told to commit. Automatically reconnects as needed.
 */
public class JMSWriter {
    private static final Logger log = LoggerFactory.getLogger(JMSWriter.class);

    // Configs
    private String userName;
    private String password;

    // JMS factory and context
    private MQConnectionFactory mqConnFactory;
    private JMSContext jmsCtxt;
    private JMSProducer jmsProd;
    private MQQueue queue;
    private int deliveryMode = Message.DEFAULT_DELIVERY_MODE;
    private long timeToLive = Message.DEFAULT_TIME_TO_LIVE;

    private boolean connected = false;  // Whether connected to MQ
    private boolean inflight = false;   // Whether messages in-flight in current transaction

    private MessageBuilder builder;

    public JMSWriter() {
    }

    /**
     * Configure this class.
     * 
     * @param props initial configuration
     *
     * @throws ConnectException   Operation failed and connector should stop.
     */
    public void configure(Map<String, String> props) {
        String queueManager = props.get(MQSinkConnector.CONFIG_NAME_MQ_QUEUE_MANAGER);
        String connectionNameList = props.get(MQSinkConnector.CONFIG_NAME_MQ_CONNECTION_NAME_LIST);
        String channelName = props.get(MQSinkConnector.CONFIG_NAME_MQ_CHANNEL_NAME);
        String queueName = props.get(MQSinkConnector.CONFIG_NAME_MQ_QUEUE);
        String userName = props.get(MQSinkConnector.CONFIG_NAME_MQ_USER_NAME);
        String password = props.get(MQSinkConnector.CONFIG_NAME_MQ_PASSWORD);
        String builderClass = props.get(MQSinkConnector.CONFIG_NAME_MQ_MESSAGE_BUILDER);
        String mbj = props.get(MQSinkConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS);
        String timeToLive = props.get(MQSinkConnector.CONFIG_NAME_MQ_TIME_TO_LIVE);
        String persistent = props.get(MQSinkConnector.CONFIG_NAME_MQ_PERSISTENT);
        String sslCipherSuite = props.get(MQSinkConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE);
        String sslPeerName = props.get(MQSinkConnector.CONFIG_NAME_MQ_SSL_PEER_NAME);

        try {
            mqConnFactory = new MQConnectionFactory();
            mqConnFactory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
            mqConnFactory.setQueueManager(queueManager);
            mqConnFactory.setConnectionNameList(connectionNameList);
            mqConnFactory.setChannel(channelName);

            queue = new MQQueue(queueName);

            this.userName = userName;
            this.password = password;
    
            queue.setMessageBodyStyle(WMQConstants.WMQ_MESSAGE_BODY_MQ);
            if (mbj != null) {
                if (Boolean.parseBoolean(mbj)) {
                    queue.setMessageBodyStyle(WMQConstants.WMQ_MESSAGE_BODY_JMS);
                }
            }

            if (timeToLive != null) {
                this.timeToLive = Long.parseLong(timeToLive);
            }
            if (persistent != null) {
                this.deliveryMode = Boolean.parseBoolean(persistent) ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
            }

            if (sslCipherSuite != null) {
                mqConnFactory.setSSLCipherSuite(sslCipherSuite);
                if (sslPeerName != null)
                {
                    mqConnFactory.setSSLPeerName(sslPeerName);
                }
            }
        }
        catch (JMSException | JMSRuntimeException jmse) {
            log.debug("JMS exception {}", jmse);
            throw new ConnectException(jmse);
        }

        try {
            Class<? extends MessageBuilder> c = Class.forName(builderClass).asSubclass(MessageBuilder.class);
            builder = c.newInstance();
        }
        catch (ClassNotFoundException | IllegalAccessException | InstantiationException | NullPointerException exc) {
            log.debug("Could not instantiate message builder {}", builderClass);
            throw new ConnectException("Could not instantiate message builder", exc);
        }
    }

    /**
     * Connects to MQ.
     */
    public void connect() {
        try {
            if (userName != null) {
                jmsCtxt = mqConnFactory.createContext(userName, password, JMSContext.SESSION_TRANSACTED);
            }
            else {
                jmsCtxt = mqConnFactory.createContext(JMSContext.SESSION_TRANSACTED);
            }            

            jmsProd = jmsCtxt.createProducer();
            jmsProd.setDeliveryMode(deliveryMode);
            jmsProd.setTimeToLive(timeToLive);
            connected = true;

            log.info("Connection to MQ established");
        }
        catch (JMSRuntimeException jmse) {
            log.info("Connection to MQ could not be established");
            log.debug("JMS exception {}", jmse);
            handleException(jmse);
        }
    }

    /**
     * Sends a message to MQ. Adds the message to the current transaction. Reconnects to MQ if required.
     * 
     * @param r                  The message and schema to send
     *
     * @throws RetriableException Operation failed, but connector should continue to retry.
     * @throws ConnectException   Operation failed and connector should stop.
     */
    public void send(SinkRecord r) throws ConnectException, RetriableException {
        connectInternal();

        try {
            Message m = builder.fromSinkRecord(jmsCtxt, r);
            inflight = true;
            jmsProd.send(queue, m);
        }
        catch (JMSRuntimeException jmse) {
            log.debug("JMS exception {}", jmse);
            throw handleException(jmse);
        }
    }


    /**
     * Commits the current transaction.
     *
     * @throws RetriableException Operation failed, but connector should continue to retry.
     * @throws ConnectException   Operation failed and connector should stop.
     */
    public void commit() throws ConnectException, RetriableException {
        connectInternal();
        try {
            if (inflight) {
                inflight = false;
            }

            jmsCtxt.commit();
        }
        catch (JMSRuntimeException jmse) {
            log.debug("JMS exception {}", jmse);
            throw handleException(jmse);
        }
    }

    /**
     * Closes the connection.
     */
    public void close() {
        try {
            inflight = false;
            connected = false;

            if (jmsCtxt != null) {
                jmsCtxt.close();
            }
        }
        catch (JMSRuntimeException jmse) {
            ;
        }
        finally
        {
            jmsCtxt = null;
            log.debug("Connection to MQ closed");
        }
    }

    /**
     * Internal method to connect to MQ.
     *
     * @throws RetriableException Operation failed, but connector should continue to retry.
     * @throws ConnectException   Operation failed and connector should stop.
     */
    private void connectInternal() throws ConnectException, RetriableException {
        if (connected) {
            return;
        }
    
        try {
            if (userName != null) {
                jmsCtxt = mqConnFactory.createContext(userName, password, JMSContext.SESSION_TRANSACTED);
            }
            else {
                jmsCtxt = mqConnFactory.createContext(JMSContext.SESSION_TRANSACTED);
            }            

            jmsProd = jmsCtxt.createProducer();
            jmsProd.setDeliveryMode(deliveryMode);
            jmsProd.setTimeToLive(timeToLive);
            connected = true;
        }
        catch (JMSRuntimeException jmse) {
            log.debug("JMS exception {}", jmse);
            throw handleException(jmse);
        }
    }

    /**
     * Handles exceptions from MQ. Some JMS exceptions are treated as retriable meaning that the
     * connector can keep running and just trying again is likely to fix things.
     */
    private ConnectException handleException(Throwable exc) {
        boolean isRetriable = false;
        boolean mustClose = true;
        int reason = -1;

        // Try to extract the MQ reason code to see if it's a retriable exception
        Throwable t = exc.getCause();
        while (t != null) {
            if (t instanceof MQException) {
                MQException mqe = (MQException)t;
                log.error("MQ error: CompCode {}, Reason {}", mqe.getCompCode(), mqe.getReason());
                reason = mqe.getReason();
                break;
            }
            t = t.getCause();
        }

        switch (reason)
        {
            // These reason codes indicate that the connection needs to be closed, but just retrying later
            // will probably recover
            case MQConstants.MQRC_BACKED_OUT:
            case MQConstants.MQRC_CHANNEL_NOT_AVAILABLE:
            case MQConstants.MQRC_CONNECTION_BROKEN:
            case MQConstants.MQRC_HOST_NOT_AVAILABLE:
            case MQConstants.MQRC_NOT_AUTHORIZED:
            case MQConstants.MQRC_Q_MGR_NOT_AVAILABLE:
            case MQConstants.MQRC_Q_MGR_QUIESCING:
            case MQConstants.MQRC_Q_MGR_STOPPING:
            case MQConstants.MQRC_UNEXPECTED_ERROR:
                isRetriable = true;
                break;

            // These reason codes indicates that the connect is still OK, but just retrying later
            // will probably recover - possibly with administrative action on the queue manager
            case MQConstants.MQRC_Q_FULL:
            case MQConstants.MQRC_PUT_INHIBITED:
                isRetriable = true;
                mustClose = false;
                break;
        }

        if (mustClose) {
            close();
        }

        if (isRetriable) {
            return new RetriableException(exc);
        }

        return new ConnectException(exc);
    }
}