/*
 * Copyright (c) 2018 Board of Trustees of Leland Stanford Jr. University,
 * all rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 * STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
 * IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * Except as contained in this notice, the name of Stanford University shall not
 * be used in advertising or otherwise to promote the sale, use or other dealings
 * in this Software without prior written authorization from Stanford University.
 */

package org.lockss.laaws.rs.util;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.*;

import org.lockss.util.test.LockssTestCase5;

class TestJmsConsumer extends LockssTestCase5 {


  static String BROKER_URI = "tcp://localhost:61616";

  private static BrokerService broker;
  private MyPublisher publisher;
  private MyMessageListener listener;
  private static String CLIENT_ID = "client";
  private static String PUB_ID = "pubsub";
  private static String TOPIC = "pubsub.t";

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    if (broker != null) {
      broker.stop();
    }
  }

  @Test
  public void testCreateTopicConsumer()  throws Exception {
    listener = new MyMessageListener("listenerConsumer");
    String testMsg = "Hello World.";
    JmsConsumer consumer= JmsConsumer.createTopicConsumer(CLIENT_ID,
        TOPIC).setListener(listener).setConnectUri(BROKER_URI);
    assertNotNull(consumer);
    consumer.connect();
    assertNull(consumer.getConnection());
    broker = new BrokerService();
    broker.addConnector(BROKER_URI);
    broker.start();
    publisher = new MyPublisher();
    publisher.create(PUB_ID, TOPIC);
    consumer.connect();
    assertNotNull(consumer.getConnection());
  }


  private static class MyMessageListener
      extends JmsConsumer.SubscriptionListener {

    String expectedMsg;

    MyMessageListener(String listenerName) {
      super(listenerName);
    }

    @Override
    public void onMessage(Message message) {
      try {
        Assertions.assertEquals(expectedMsg,
            JmsConsumer.convertMessage(message));
      } catch (JMSException e) {
        Assertions.fail("Exception converting message", e);
      }
    }

    void setExpected(String s) {
      expectedMsg = s;
    }
  }

  public static class MyPublisher {

    private String clientId;
    private Connection connection;
    private Session session;
    private MessageProducer messageProducer;

    public void create(String clientId, String topicName)
        throws JMSException {
      this.clientId = clientId;

      // create a Connection Factory
      ConnectionFactory connectionFactory =
          new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_BROKER_URL);

      // create a Connection
      connection = connectionFactory.createConnection();
      connection.setClientID(clientId);

      // create a Session
      session =
          connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // create the Topic to which messages will be sent
      Topic topic = session.createTopic(topicName);

      // create a MessageProducer for sending messages
      messageProducer = session.createProducer(topic);
    }

    public void closeConnection() throws JMSException {
      if (connection != null) {
        connection.close();
      }
    }

    /**
     * Send a text message.
     *
     * @param text the text string to send
     * @throws JMSException if the TextMessage is cannot be created.
     */
    public void sendText(String text) throws JMSException {
      // create a JMS TextMessage
      TextMessage textMessage = session.createTextMessage(text);

      // send the message to the topic destination
      messageProducer.send(textMessage);
    }
  }

}
