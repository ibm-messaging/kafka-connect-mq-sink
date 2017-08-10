/**
 * Copyright 2017 IBM Corporation
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
import com.ibm.msg.client.wmq.WMQConstants;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;

import org.apache.kafka.connect.data.Schema;
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
    private boolean messageBodyJms;

    // JMS factory and context
    private MQConnectionFactory mqConnFactory;
    private JMSContext jmsCtxt;
    private JMSProducer jmsProd;
    private MQQueue queue;
    private int deliveryMode = Message.DEFAULT_DELIVERY_MODE;
    private long timeToLive = Message.DEFAULT_TIME_TO_LIVE;

    private boolean connected = false;  // Whether connected to MQ
    private boolean inflight = false;   // Whether messages in-flight in current transaction

    /**
     * Constructor.
     *
     * @param queueManager       Queue manager name
     * @param connectionNameList Connection name list, comma-separated list of host(port) entries
     * @param channelName        Server-connection channel name
     * @param queueName          Queue name
     * @param userName           User name for authenticating to MQ, can be null
     * @param password           Password for authenticating to MQ, can be null
     *
     * @throws ConnectException   Operation failed and connector should stop.
     */
    public JMSWriter(String queueManager, String connectionNameList, String channelName, String queueName, String userName, String password) throws ConnectException {
        this.userName = userName;
        this.password = password;

        try {
            mqConnFactory = new MQConnectionFactory();
            mqConnFactory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
            mqConnFactory.setQueueManager(queueManager);
            mqConnFactory.setConnectionNameList(connectionNameList);
            mqConnFactory.setChannel(channelName);

            queue = new MQQueue(queueName);
            messageBodyJms = false;
            queue.setMessageBodyStyle(WMQConstants.WMQ_MESSAGE_BODY_MQ);
        }
        catch (JMSException | JMSRuntimeException jmse) {
            log.debug("JMS exception {}", jmse);
            throw new ConnectException(jmse);
        }
    }

    /**
     * Setter for message body as JMS.
     *
     * @param messageBodyJms     Whether to interpret the message body as a JMS message type
     */
    public void setMessageBodyJms(boolean messageBodyJms)
    {
        if (messageBodyJms != this.messageBodyJms) {
            this.messageBodyJms = messageBodyJms;
            try {
                if (!messageBodyJms) {
                    queue.setMessageBodyStyle(WMQConstants.WMQ_MESSAGE_BODY_MQ);
                }
                else {
                    queue.setMessageBodyStyle(WMQConstants.WMQ_MESSAGE_BODY_JMS);
                }
            }
            catch (JMSException jmse) {
                ;
            }
        }
    }

    /**
     * Setter for message persistence.
     *
     * @param persistent         true for persistent, false for non-persistent
     */
    public void setPersistent(boolean persistent)
    {
        this.deliveryMode = persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
    }

    /**
     * Setter for message time-to-live.
     *
     * @param timeToLive         Time-to-live in milliseconds, 0 for unlimited
     */
    public void setTimeToLive(long timeToLive)
    {
        this.timeToLive = timeToLive;
    }

    /**
     * Connects to MQ.
     *
     * @throws RetriableException Operation failed, but connector should continue to retry.
     * @throws ConnectException   Operation failed and connector should stop.
     */
    public void connect() throws ConnectException, RetriableException {
        connectInternal();
        log.info("Connection to MQ established");
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
            Message m;
            Schema s = r.valueSchema();

            log.trace("Value schema {}", s);
            if (s == null) {
                log.trace("No schema info {}", r.value());
                if (r.value() != null) {
                    m = jmsCtxt.createTextMessage(r.value().toString());
                }
                else {
                    m = jmsCtxt.createTextMessage();
                }
            }
            else if (s.type().isPrimitive())
            {
                switch(s.type()) {
                    case STRING:
                        m = jmsCtxt.createTextMessage((String)(r.value()));
                        break;
                    case BYTES:
                        BytesMessage bm = jmsCtxt.createBytesMessage();
                        bm.writeBytes((byte[])(r.value()));
                        m = bm;
                        break;
                    default:
                        log.debug("Unsupported primitive data type", s);
                        throw new ConnectException("Unsupported primitive data type");
                }
            }
            else {
                log.trace("Compound schema {}", s);
                m = jmsCtxt.createTextMessage(r.value().toString());
            }

            inflight = true;
            jmsProd.send(queue, m);
        }
        catch (JMSException | JMSRuntimeException jmse) {
            log.debug("JMS exception {}", jmse);
            handleException(jmse);
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
            handleException(jmse);
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
            handleException(jmse);
        }
    }

    /**
     * Handles exceptions from MQ. Some JMS exceptions are treated as retriable meaning that the
     * connector can keep running and just trying again is likely to fix things.
     */
    private void handleException(Throwable exc) throws ConnectException, RetriableException {
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
            throw new RetriableException(exc);
        }
        throw new ConnectException(exc);
    }
}