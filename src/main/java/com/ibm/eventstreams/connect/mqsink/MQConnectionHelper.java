/**
 * Copyright 2023, 2024 IBM Corporation
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

import javax.jms.JMSException;
import javax.net.ssl.SSLContext;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;

import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;

public class MQConnectionHelper {
    private AbstractConfig config;

    public String getQueueManagerName() {
        return config.getString(MQSinkConfig.CONFIG_NAME_MQ_QUEUE_MANAGER);
    }

    public String getQueueName() {
        return config.getString(MQSinkConfig.CONFIG_NAME_MQ_QUEUE);
    }

    public String getStateQueueName() {
        return config.getString(MQSinkConfig.CONFIG_NAME_MQ_EXACTLY_ONCE_STATE_QUEUE);
    }

    public String getUserName() {
        return config.getString(MQSinkConfig.CONFIG_NAME_MQ_USER_NAME);
    }

    public Password getPassword() {
        return config.getPassword(MQSinkConfig.CONFIG_NAME_MQ_PASSWORD);
    }

    public boolean isMessageBodyJms() {
        return config.getBoolean(MQSinkConfig.CONFIG_NAME_MQ_MESSAGE_BODY_JMS);
    }

    public Long getTimeToLive() {
        return config.getLong(MQSinkConfig.CONFIG_NAME_MQ_TIME_TO_LIVE);
    }

    public Boolean isPersistent() {
        return config.getBoolean(MQSinkConfig.CONFIG_NAME_MQ_PERSISTENT);
    }

    public String getUseIBMCipherMappings() {
        return config.getString(MQSinkConfig.CONFIG_NAME_MQ_SSL_USE_IBM_CIPHER_MAPPINGS);
    }

    public int getTransportType() {
        return getTransportType(config.getString(MQSinkConfig.CONFIG_NAME_MQ_CONNECTION_MODE));
    }

    public MQConnectionHelper(final AbstractConfig config) {
        this.config = config;
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
    public MQConnectionFactory createMQConnFactory() throws JMSException, MalformedURLException {
        final MQConnectionFactory mqConnFactory = new MQConnectionFactory();
        final int transportType = getTransportType();
        mqConnFactory.setTransportType(transportType);
        mqConnFactory.setQueueManager(getQueueManagerName());
        mqConnFactory.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, 
            config.getBoolean(MQSinkConfig.CONFIG_NAME_MQ_USER_AUTHENTICATION_MQCSP));

        if (transportType == WMQConstants.WMQ_CM_CLIENT) {
            final String ccdtUrl = config.getString(MQSinkConfig.CONFIG_NAME_MQ_CCDT_URL);
            if (ccdtUrl != null) {
                mqConnFactory.setCCDTURL(new URL(ccdtUrl));
            } else {
                mqConnFactory.setConnectionNameList(config.getString(MQSinkConfig.CONFIG_NAME_MQ_CONNECTION_NAME_LIST));
                mqConnFactory.setChannel(config.getString(MQSinkConfig.CONFIG_NAME_MQ_CHANNEL_NAME));
            }

            mqConnFactory.setSSLCipherSuite(config.getString(MQSinkConfig.CONFIG_NAME_MQ_SSL_CIPHER_SUITE));
            mqConnFactory.setSSLPeerName(config.getString(MQSinkConfig.CONFIG_NAME_MQ_SSL_PEER_NAME));
            
            final String sslKeystoreLocation = config.getString(MQSinkConfig.CONFIG_NAME_MQ_SSL_KEYSTORE_LOCATION);
            final String sslTruststoreLocation = config.getString(MQSinkConfig.CONFIG_NAME_MQ_SSL_TRUSTSTORE_LOCATION);
            if (sslKeystoreLocation != null || sslTruststoreLocation != null) {
                final SSLContext sslContext = new SSLContextBuilder().buildSslContext(sslKeystoreLocation,
                        config.getPassword(MQSinkConfig.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD), sslTruststoreLocation, config.getPassword(MQSinkConfig.CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD));
                mqConnFactory.setSSLSocketFactory(sslContext.getSocketFactory());
            }
        }
        return mqConnFactory;
    }
}
