/**
 * Copyright 2022 IBM Corporation
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

import java.util.concurrent.TimeoutException;

import javax.jms.JMSContext;

import org.junit.ClassRule;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.WaitingConsumer;

import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;


/**
 * Helper class for integration tests that have a dependency on JMSContext.
 * 
 *  It starts a queue manager in a test container, and uses it to create
 *  a JMSContext instance, that can be used in tests.
 */
public abstract class AbstractJMSContextIT {

    private static final String QMGR_NAME = "MYQMGR";
    
    @ClassRule
    public static GenericContainer<?> MQ_CONTAINER = new GenericContainer<>("icr.io/ibm-messaging/mq:latest")
        .withEnv("LICENSE", "accept")
        .withEnv("MQ_QMGR_NAME", QMGR_NAME)
        .withEnv("MQ_ENABLE_EMBEDDED_WEB_SERVER", "false")
        .withExposedPorts(1414);
    
    private JMSContext jmsContext;
    
    
    /**
     * Returns a JMS context pointing at a developer queue manager running in a 
     * test container. 
     */
    public JMSContext getJmsContext() throws Exception {
        if (jmsContext == null) {
            waitForQueueManagerStartup();
            
            MQConnectionFactory mqcf = new MQConnectionFactory();
            mqcf.setTransportType(WMQConstants.WMQ_CM_CLIENT);
            mqcf.setChannel("DEV.APP.SVRCONN");
            mqcf.setQueueManager(QMGR_NAME);
            mqcf.setConnectionNameList("localhost(" + getMQPort().toString() + ")");
         
            jmsContext = mqcf.createContext();
        }
        
        return jmsContext;
    }
    
    
    /**
     * Gets the host port that has been mapped to the default MQ 1414 port in the test container.
     */
    private Integer getMQPort() {
        return MQ_CONTAINER.getMappedPort(1414);                
    }

    /** 
     * Waits until we see a log line in the queue manager test container that indicates
     *  the queue manager is ready.
     */
    private void waitForQueueManagerStartup() throws TimeoutException {
        WaitingConsumer logConsumer = new WaitingConsumer();
        MQ_CONTAINER.followOutput(logConsumer);
        logConsumer.waitUntil(logline -> logline.getUtf8String().contains("AMQ5975I"));
    }    
}
