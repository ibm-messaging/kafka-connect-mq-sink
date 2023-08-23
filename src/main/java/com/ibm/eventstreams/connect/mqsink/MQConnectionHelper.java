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
package com.ibm.eventstreams.connect.mqsink;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import javax.jms.JMSException;
import javax.net.ssl.SSLContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;

public class MQConnectionHelper {
    private static final Logger log = LoggerFactory.getLogger(JMSWorker.class);
    private String queueManager;
    private String connectionNameList;
    private String channelName;
    private String ccdtUrl;
    private String sslCipherSuite;
    private String sslPeerName;
    private String sslKeystoreLocation;
    private String sslKeystorePassword;
    private String sslTruststoreLocation;
    private String sslTruststorePassword;
    private String useMQCSP;
    private int transportType;
    private String connectionMode;
    private String queueName;
    private String stateQueueName;
    private String userName;
    private String password;
    private String mbj;
    private String timeToLive;
    private String persistent;
    private String useIBMCipherMappings;

    public String getQueueName() {
        return queueName;
    }

    public String getStateQueueName() {
        return stateQueueName;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public String getMbj() {
        return mbj;
    }

    public String getTimeToLive() {
        return timeToLive;
    }

    public String getPersistent() {
        return persistent;
    }

    public String getConnectionMode() {
        return connectionMode;
    }

    public String getUseIBMCipherMappings() {
        return useIBMCipherMappings;
    }

    public MQConnectionHelper(final Map<String, String> props) {
        queueManager = props.get(MQSinkConfig.CONFIG_NAME_MQ_QUEUE_MANAGER);
        connectionMode = props.get(MQSinkConfig.CONFIG_NAME_MQ_CONNECTION_MODE);
        connectionNameList = props.get(MQSinkConfig.CONFIG_NAME_MQ_CONNECTION_NAME_LIST);
        channelName = props.get(MQSinkConfig.CONFIG_NAME_MQ_CHANNEL_NAME);
        queueName = props.get(MQSinkConfig.CONFIG_NAME_MQ_QUEUE);
        stateQueueName = props.get(MQSinkConfig.CONFIG_NAME_MQ_EXACTLY_ONCE_STATE_QUEUE);
        userName = props.get(MQSinkConfig.CONFIG_NAME_MQ_USER_NAME);
        password = props.get(MQSinkConfig.CONFIG_NAME_MQ_PASSWORD);
        ccdtUrl = props.get(MQSinkConfig.CONFIG_NAME_MQ_CCDT_URL);
        mbj = props.get(MQSinkConfig.CONFIG_NAME_MQ_MESSAGE_BODY_JMS);
        timeToLive = props.get(MQSinkConfig.CONFIG_NAME_MQ_TIME_TO_LIVE);
        persistent = props.get(MQSinkConfig.CONFIG_NAME_MQ_PERSISTENT);
        sslCipherSuite = props.get(MQSinkConfig.CONFIG_NAME_MQ_SSL_CIPHER_SUITE);
        sslPeerName = props.get(MQSinkConfig.CONFIG_NAME_MQ_SSL_PEER_NAME);
        sslKeystoreLocation = props.get(MQSinkConfig.CONFIG_NAME_MQ_SSL_KEYSTORE_LOCATION);
        sslKeystorePassword = props.get(MQSinkConfig.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD);
        sslTruststoreLocation = props.get(MQSinkConfig.CONFIG_NAME_MQ_SSL_TRUSTSTORE_LOCATION);
        sslTruststorePassword = props.get(MQSinkConfig.CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD);
        useMQCSP = props.get(MQSinkConfig.CONFIG_NAME_MQ_USER_AUTHENTICATION_MQCSP);
        useIBMCipherMappings = props.get(MQSinkConfig.CONFIG_NAME_MQ_SSL_USE_IBM_CIPHER_MAPPINGS);

        transportType = getTransportType(connectionMode);
    }

    /**
     * Get the transport type from the connection mode.
     *
     * @param connectionMode
     *                       the connection mode
     * @return the transport type
     * @throws ConnectException
     *                          if the connection mode is not supported
     */
    public static int getTransportType(final String connectionMode) {
        int transportType = WMQConstants.WMQ_CM_CLIENT;
        if (connectionMode != null) {
            if (connectionMode.equals(MQSinkConfig.CONFIG_VALUE_MQ_CONNECTION_MODE_CLIENT)) {
                transportType = WMQConstants.WMQ_CM_CLIENT;
            } else if (connectionMode.equals(MQSinkConfig.CONFIG_VALUE_MQ_CONNECTION_MODE_BINDINGS)) {
                transportType = WMQConstants.WMQ_CM_BINDINGS;
            } else {
                log.error("Unsupported MQ connection mode {}", connectionMode);
                throw new JMSWorkerConnectionException("Unsupported MQ connection mode");
            }
        }
        return transportType;
    }

    /**
     * * Create a MQ connection factory.
     *
     * The connection factory is configured with the supplied properties.
     *
     * @return
     * @throws JMSException
     */
    public MQConnectionFactory createMQConnFactory() throws JMSException {
        final MQConnectionFactory mqConnFactory = new MQConnectionFactory();
        mqConnFactory.setTransportType(transportType);
        mqConnFactory.setQueueManager(queueManager);
        mqConnFactory.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true);
        if (useMQCSP != null) {
            mqConnFactory.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, Boolean.parseBoolean(useMQCSP));
        }

        if (transportType == WMQConstants.WMQ_CM_CLIENT) {
            if (ccdtUrl != null) {
                final URL ccdtUrlObject;
                try {
                    ccdtUrlObject = new URL(ccdtUrl);
                } catch (final MalformedURLException e) {
                    log.error("MalformedURLException exception {}", e);
                    throw new JMSWorkerConnectionException("CCDT file url invalid", e);
                }
                mqConnFactory.setCCDTURL(ccdtUrlObject);
            } else {
                mqConnFactory.setConnectionNameList(connectionNameList);
                mqConnFactory.setChannel(channelName);
            }

            if (sslCipherSuite != null) {
                mqConnFactory.setSSLCipherSuite(sslCipherSuite);
                if (sslPeerName != null) {
                    mqConnFactory.setSSLPeerName(sslPeerName);
                }
            }

            if (sslKeystoreLocation != null || sslTruststoreLocation != null) {
                final SSLContext sslContext = new SSLContextBuilder().buildSslContext(sslKeystoreLocation,
                        sslKeystorePassword, sslTruststoreLocation, sslTruststorePassword);
                mqConnFactory.setSSLSocketFactory(sslContext.getSocketFactory());
            }
        }
        return mqConnFactory;
    }
}
