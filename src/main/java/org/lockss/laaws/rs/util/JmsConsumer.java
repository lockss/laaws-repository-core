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

import com.nurkiewicz.asyncretry.AsyncRetryExecutor;
import com.nurkiewicz.asyncretry.RetryExecutor;
import java.io.Serializable;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class JmsConsumer {

  private static final Log log = LogFactory.getLog(JmsConsumer.class);

  protected String mClientId;
  protected Connection mConnection;
  protected MessageConsumer mMessageConsumer;
  protected Session mSession;
  protected String mConnectUri;
  protected String mTopicName;
  protected MessageListener mMsgListener;
  protected ScheduledExecutorService mScheduler;

  public static JmsConsumer createTopicConsumer(String clientId,
      String topicName)
      throws JMSException {
    return JmsConsumer.createTopicConsumer(clientId, topicName, null);
  }

  public static JmsConsumer createTopicConsumer(String clientId,
      String topicName,
      MessageListener listener)
      throws JMSException {
    JmsConsumer res = new JmsConsumer();
    res.createTopic(clientId, topicName, listener);
    return res;
  }

  private JmsConsumer createTopic(String clientId,
      String topicName,
      MessageListener listener)
      throws JMSException {

    mClientId = clientId;
    mTopicName = topicName;
    mMsgListener = listener;
    return this;
  }

  public JmsConsumer setConnectUri(String uri) {
    mConnectUri = uri;
    return this;
  }

  public String getConnectUri() {
    return mConnectUri;
  }

  protected Connection getConnection() {
    return mConnection;
  }

  public JmsConsumer connect() throws JMSException {

    mScheduler = Executors.newSingleThreadScheduledExecutor();

    RetryExecutor executor = new AsyncRetryExecutor(mScheduler).
        abortOn(NullPointerException.class).
        retryOn(JMSException.class).
        withExponentialBackoff(10, 2).
        withUniformJitter(). //add between +/- 100 ms randomly
        withMaxRetries(20);

    CompletableFuture<Connection> futureCon = executor.
        getWithRetry(() -> startConsumer());
      futureCon.whenComplete((con, ex) -> {
        if (con !=null) {
          mConnection = con;
        }
        else if (ex != null) {
          log.error("Unable to establish connection: " + ex.getMessage());
        }
      });

    return this;
  }


  private Connection  startConsumer() throws JMSException {
    String uri = getConnectUri();
    // create a Connection Factory
    log.debug("Creating consumer for topic: " + mTopicName +
        ", client: " + mClientId + " at " + uri);
    ConnectionFactory connectionFactory =
        new ActiveMQConnectionFactory(uri);
    mConnection = connectionFactory.createConnection();
    // we have a valid mConnection
    mConnection.setClientID(mClientId);
    if (log.isTraceEnabled()) {
      log.trace("Created mSession for topic: " + mTopicName +
          ", client: " + mClientId + " at " + uri);
    }
    // create a Session
    mSession = mConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

    // create the Topic from which messages will be received
    Topic topic = mSession.createTopic(mTopicName);

    // create a MessageConsumer for receiving messages
    mMessageConsumer = mSession.createConsumer(topic);
    if (mMsgListener != null) {
      mMessageConsumer.setMessageListener(mMsgListener);
    }
    // start the mConnection in order to receive messages
    mConnection.start();
    return mConnection;
  }

  public void closeConnection() throws JMSException {
    if (mConnection != null) {
      mConnection.close();
    }
  }

  public JmsConsumer setListener(MessageListener listener) throws JMSException {
    mMsgListener = listener;

    if (mMessageConsumer != null) {
      mMessageConsumer.setMessageListener(listener);
    }
    return this;
  }

  public Object receive(int timeout) throws JMSException {
    Message message = mMessageConsumer.receive(timeout);
    if (message != null) {
      return convertMessage(message);
    }
    return null;
  }

  /**
   * This implementation converts a Message to the underlying type.
   * TextMessage back to a String, a ByteMessage back to a byte array,
   * a MapMessage back to a Map, and an ObjectMessage back to a Serializable object. Returns
   * the plain Message object in case of an unknown message type.
   */
  public static Object convertMessage(Message message) throws JMSException {
    if (message instanceof TextMessage) {
      return ((TextMessage) message).getText();
    }
    else if (message instanceof BytesMessage) {
      BytesMessage bytesMessage = (BytesMessage) message;
      byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
      bytesMessage.readBytes(bytes);
      return bytes;
    }
    else if (message instanceof MapMessage) {
      MapMessage mapMessage = (MapMessage) message;
      Map<String, Object> map = new HashMap<>();
      Enumeration<String> en = mapMessage.getMapNames();
      while (en.hasMoreElements()) {
        String key = en.nextElement();
        map.put(key, mapMessage.getObject(key));
      }
      return map;
    }
    else if (message instanceof ObjectMessage) {
      return ((ObjectMessage) message).getObject();
    }
    else {
      return message;
    }
  }

  /**
   * Receive a text message from the message queue.
   *
   * @param timeout the time to wait for the message to be received.
   * @return the resulting String message.
   * @throws JMSException if thrown by JMS methods
   */
  public String receiveText(int timeout) throws JMSException {
    Object received = receive(timeout);

    // check if a message was received
    if (received instanceof String) {
      // cast the message to the correct type
      String text = (String) received;
      if (log.isDebugEnabled()) {
        log.debug(mClientId + ": received text ='" + text + "'");
      }
      return text;
    }
    else {
      log.debug(mClientId + ": String message not received");
    }
    return null;
  }


  /**
   * Return a Map with string keys and object values from the message queue.
   *
   * @param timeout the time to wait for the message to be received.
   * @return the resulting Map
   * @throws JMSException if thrown by JMS methods
   */
  @SuppressWarnings("unchecked")
  public Map<String, Object> receiveMap(int timeout) throws JMSException {
    Object received = receive(timeout);
    // check if a message was received
    if (received != null && received instanceof Map) {
      log.debug(mClientId + ": received map.");
      return (Map<String, Object>) received;
    }
    else {
      log.debug(mClientId + ": Map not received");
    }
    return null;
  }

  /**
   * Return a byte array from the message queue
   *
   * @param timeout the time to wait for the message to be received.
   * @return the byte array.
   * @throws JMSException if thrown by JMS methods
   */
  public byte[] receiveBytes(int timeout) throws JMSException {
    Object received = receive(timeout);
    if (received instanceof byte[]) {
      byte[] bytes = (byte[]) received;
      if (log.isDebugEnabled()) {
        log.debug(mClientId + ": received bytes ='" + bytes + "'");
      }
      return bytes;
    }
    else {
      log.debug(mClientId + ": no bytes received");
    }
    return new byte[0];
  }

  /**
   * Return a serializable object from the message queue.
   *
   * @param timeout for the message consumer receive
   * @return the resulting Serializable object
   * @throws JMSException if thrown by JMS methods
   */
  public Serializable receiveObject(int timeout)
      throws JMSException {
    Object received = receive(timeout);
    if (received != null && received instanceof Serializable) {
      Serializable obj = (Serializable) received;
      if (log.isDebugEnabled()) {
        log.debug(mClientId + ": received serializable object ='" +
            obj.toString() + "'");
      }
      return obj;
    }
    else {
      if(log.isDebugEnabled())
        log.debug(mClientId + ": no message received");
    }
    return null;
  }

  /**
   * A Basic MessageListener.  Override the onMessage to appropriate functionality.
   */
  public abstract static class SubscriptionListener implements MessageListener {

    protected String listenerName;

    public SubscriptionListener(String listenerName) {
      this.listenerName = listenerName;
    }

    String getListenerName() {
      return listenerName;
    }

  }

}
