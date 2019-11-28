/**
 * Copyright 2017, 2018, 2019 IBM Corporation
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

import com.ibm.mq.MQException;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.jms.*;
import com.ibm.eventstreams.connect.mqsink.builders.MessageBuilder;
import com.ibm.msg.client.wmq.WMQConstants;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.*;
import java.util.Map;

import javax.jms.DeliveryMode;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

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

    private MessageBuilder builder;

    private boolean connected = false;                              // Whether connected to MQ
    private boolean inflight = false;                               // Whether messages in-flight in current transaction
    private long reconnectDelayMillis = RECONNECT_DELAY_MILLIS_MIN; // Delay between repeated reconnect attempts

    private static long RECONNECT_DELAY_MILLIS_MIN = 64l;
    private static long RECONNECT_DELAY_MILLIS_MAX = 8192l;

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
        log.trace("[{}] Entry {}.configure, props={}", Thread.currentThread().getId(), this.getClass().getName(), props);

        String queueManager = props.get(MQSinkConnector.CONFIG_NAME_MQ_QUEUE_MANAGER);
        String connectionMode = props.get(MQSinkConnector.CONFIG_NAME_MQ_CONNECTION_MODE);
        String connectionNameList = props.get(MQSinkConnector.CONFIG_NAME_MQ_CONNECTION_NAME_LIST);
        String channelName = props.get(MQSinkConnector.CONFIG_NAME_MQ_CHANNEL_NAME);
        String queueName = props.get(MQSinkConnector.CONFIG_NAME_MQ_QUEUE);
        String userName = props.get(MQSinkConnector.CONFIG_NAME_MQ_USER_NAME);
        String password = props.get(MQSinkConnector.CONFIG_NAME_MQ_PASSWORD);
        String ccdtUrl = props.get(MQSinkConnector.CONFIG_NAME_MQ_CCDT_URL);
        String builderClass = props.get(MQSinkConnector.CONFIG_NAME_MQ_MESSAGE_BUILDER);
        String mbj = props.get(MQSinkConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS);
        String timeToLive = props.get(MQSinkConnector.CONFIG_NAME_MQ_TIME_TO_LIVE);
        String persistent = props.get(MQSinkConnector.CONFIG_NAME_MQ_PERSISTENT);
        String sslCipherSuite = props.get(MQSinkConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE);
        String sslPeerName = props.get(MQSinkConnector.CONFIG_NAME_MQ_SSL_PEER_NAME);
        String sslKeystoreLocation = props.get(MQSinkConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_LOCATION);
        String sslKeystorePassword = props.get(MQSinkConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD);
        String sslTruststoreLocation = props.get(MQSinkConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_LOCATION);
        String sslTruststorePassword = props.get(MQSinkConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD);
        String useMQCSP = props.get(MQSinkConnector.CONFIG_NAME_MQ_USER_AUTHENTICATION_MQCSP);

        int transportType = WMQConstants.WMQ_CM_CLIENT;
        if (connectionMode != null) {
            if (connectionMode.equals(MQSinkConnector.CONFIG_VALUE_MQ_CONNECTION_MODE_CLIENT)) {
                transportType = WMQConstants.WMQ_CM_CLIENT;
            } 
            else if (connectionMode.equals(MQSinkConnector.CONFIG_VALUE_MQ_CONNECTION_MODE_BINDINGS)) {
                transportType = WMQConstants.WMQ_CM_BINDINGS;
            } 
            else {
                log.error("Unsupported MQ connection mode {}", connectionMode);
                throw new ConnectException("Unsupported MQ connection mode");
            }
        }

        try {
            mqConnFactory = new MQConnectionFactory();
            mqConnFactory.setTransportType(transportType);
            mqConnFactory.setQueueManager(queueManager);
            mqConnFactory.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true);
            if (useMQCSP != null) {
                mqConnFactory.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, Boolean.parseBoolean(useMQCSP));
            }

            if (transportType == WMQConstants.WMQ_CM_CLIENT) {
                if (ccdtUrl != null) {
                    URL ccdtUrlObject;
                    try {
                        ccdtUrlObject = new URL(ccdtUrl);
                    }
                    catch (MalformedURLException e) {
                        log.error("MalformedURLException exception {}", e);
                        throw new ConnectException("CCDT file url invalid", e);
                    }
                    mqConnFactory.setCCDTURL(ccdtUrlObject);
                } else {
                    mqConnFactory.setConnectionNameList(connectionNameList);
                    mqConnFactory.setChannel(channelName);
                }

                if (sslCipherSuite != null) {
                    mqConnFactory.setSSLCipherSuite(sslCipherSuite);
                    if (sslPeerName != null)
                    {
                        mqConnFactory.setSSLPeerName(sslPeerName);
                    }
                }

                if (sslKeystoreLocation != null || sslTruststoreLocation != null) {
                    final SSLContext sslContext = buildSslContext(sslKeystoreLocation, sslKeystorePassword, sslTruststoreLocation, sslTruststorePassword);
                    mqConnFactory.setSSLSocketFactory(sslContext.getSocketFactory());
                }
            }

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
        }
        catch (JMSException | JMSRuntimeException jmse) {
            log.error("JMS exception {}", jmse);
            throw new ConnectException(jmse);
        }

        try {
            Class<? extends MessageBuilder> c = Class.forName(builderClass).asSubclass(MessageBuilder.class);
            builder = c.newInstance();
            builder.configure(props);
        }
        catch (ClassNotFoundException | ClassCastException | IllegalAccessException | InstantiationException | NullPointerException exc) {
            log.error("Could not instantiate message builder {}", builderClass);
            throw new ConnectException("Could not instantiate message builder", exc);
        }

        log.trace("[{}]  Exit {}.configure", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Connects to MQ.
     */
    public void connect() {
        log.trace("[{}] Entry {}.connect", Thread.currentThread().getId(), this.getClass().getName());

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
            log.error("JMS exception {}", jmse);
            handleException(jmse);
        }

        log.trace("[{}]  Exit {}.connect", Thread.currentThread().getId(), this.getClass().getName());
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
        log.trace("[{}] Entry {}.send", Thread.currentThread().getId(), this.getClass().getName());

        connectInternal();

        try {
            Message m = builder.fromSinkRecord(jmsCtxt, r);
            inflight = true;
            jmsProd.send(queue, m);
        }
        catch (JMSRuntimeException jmse) {
            log.error("JMS exception {}", jmse);
            throw handleException(jmse);
        }

        log.trace("[{}]  Exit {}.send", Thread.currentThread().getId(), this.getClass().getName());
    }


    /**
     * Commits the current transaction.
     *
     * @throws RetriableException Operation failed, but connector should continue to retry.
     * @throws ConnectException   Operation failed and connector should stop.
     */
    public void commit() throws ConnectException, RetriableException {
        log.trace("[{}] Entry {}.commit", Thread.currentThread().getId(), this.getClass().getName());

        connectInternal();
        try {
            if (inflight) {
                inflight = false;
            }

            jmsCtxt.commit();
        }
        catch (JMSRuntimeException jmse) {
            log.error("JMS exception {}", jmse);
            throw handleException(jmse);
        }

        log.trace("[{}]  Exit {}.commit", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Closes the connection.
     */
    public void close() {
        log.trace("[{}] Entry {}.close", Thread.currentThread().getId(), this.getClass().getName());

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
        finally {
            jmsCtxt = null;
            log.debug("Connection to MQ closed");
        }

        log.trace("[{}]  Exit {}.close", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Internal method to connect to MQ.
     *
     * @throws RetriableException Operation failed, but connector should continue to retry.
     * @throws ConnectException   Operation failed and connector should stop.
     */
    private void connectInternal() throws ConnectException, RetriableException {
        log.trace("[{}] Entry {}.connectInternal", Thread.currentThread().getId(), this.getClass().getName());

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
            reconnectDelayMillis = RECONNECT_DELAY_MILLIS_MIN;
            connected = true;
        }
        catch (JMSRuntimeException jmse) {
            // Delay slightly so that repeated reconnect loops don't run too fast
            try {
                Thread.sleep(reconnectDelayMillis);
            }
            catch (InterruptedException ie) {
                ;
            }

            if (reconnectDelayMillis < RECONNECT_DELAY_MILLIS_MAX)
            {
                reconnectDelayMillis = reconnectDelayMillis * 2;
            }

            log.error("JMS exception {}", jmse);
            throw handleException(jmse);
        }

        log.trace("[{}]  Exit {}.connectInternal", Thread.currentThread().getId(), this.getClass().getName());
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

    private SSLContext buildSslContext(String sslKeystoreLocation, String sslKeystorePassword, String sslTruststoreLocation, String sslTruststorePassword) {
        log.trace("[{}] Entry {}.buildSslContext", Thread.currentThread().getId(), this.getClass().getName());

        try {
            final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

            if (sslKeystoreLocation != null) {
                kmf.init(loadKeyStore(sslKeystoreLocation, sslKeystorePassword), sslKeystorePassword.toCharArray());
            }

            if (sslTruststoreLocation != null) {
                tmf.init(loadKeyStore(sslTruststoreLocation, sslTruststorePassword));
            }

            final SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());

            log.trace("[{}]  Exit {}.buildSslContext, retval={}", Thread.currentThread().getId(), this.getClass().getName(), sslContext);
            return sslContext;

        } catch (GeneralSecurityException e) {
            throw new ConnectException("Error creating SSLContext", e);
        }
    }

    private KeyStore loadKeyStore(String location, String password) throws GeneralSecurityException {
        log.trace("[{}] Entry {}.loadKeyStore", Thread.currentThread().getId(), this.getClass().getName());

        try (final InputStream ksStr = new FileInputStream(location)) {
            final KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(ksStr, password.toCharArray());

            log.trace("[{}]  Exit {}.loadKeyStore, retval={}", Thread.currentThread().getId(), this.getClass().getName(), ks);
            return ks;

        } catch (IOException e) {
            throw new ConnectException("Error reading keystore " + location, e);
        }
    }
}