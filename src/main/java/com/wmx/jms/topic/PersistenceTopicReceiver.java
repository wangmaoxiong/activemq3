package com.wmx.jms.topic;
import org.apache.activemq.ActiveMQConnectionFactory;
import javax.jms.*;
/**
 * 持久化 topic(主题) 消息消费者
 */
@SuppressWarnings("all")
public class PersistenceTopicReceiver {
    public static void main(String[] args) {
        Connection connection = null;
        Session session = null;
        TopicSubscriber topicSubscriber = null;
        try {
            String brokerURL = "tcp://127.0.0.1:61616";//ActiveMQ 中间件连接地址
            ConnectionFactory mqConnectionFactory = new ActiveMQConnectionFactory(brokerURL);
            //如果 ActiveMQ 连不上，则抛异常：java.net.ConnectException: Connection refused: connect
            connection = mqConnectionFactory.createConnection();//通过连接工厂获取连接 javax.jms.Connection
            /**为持久订阅设置客户端id，这样即使订阅者不在线，消息中心也能在它下次上线时将消息投递给它*/
            connection.setClientID("clientID_100");
            //创建会话 session。开启事务，消息确认模式为自动确认
            session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
            //创建消息主题(topic)，主题名称与生产者设置的保持一致。Queue 、Topic 都是 Destination 的子接口
            Topic topic = session.createTopic("topic-app-2");
            /**createDurableSubscriber(Topic var1, String var2)：创建持久订阅，var1 是订阅对象，var2 是持久订阅名称，自定义即可
             * 主题订阅接口 TopicSubscriber 继承了 MessageConsumer 消息消费者接口*/
            topicSubscriber = session.createDurableSubscriber(topic,"ds-1");//根据目的地创建消息消费者
            connection.start();/**设置了主题订阅后，再启动连接*/
            System.out.println("订阅者启动成功...");
            Message message = topicSubscriber.receive();//receive方法会导致当前线程阻塞，直到接收到消息
            while (message != null) {
                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    System.out.println("收到消息：" + textMessage.getText());
                    session.commit();//确认消息。
                    //接收消息时设置超时时间，单位为毫秒。如果为0，则等同于 receive()一致阻塞。
                    //如果超过超时时间，仍然未接收到消息，则返回 null。while 会推出
                    message = topicSubscriber.receive(3000);
                }
            }
        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            try {
                if (session != null) {
                    session.close();//关闭会话
                }
                if (connection != null) {
                    connection.close();//关闭连接
                }
                if (topicSubscriber != null) {
                    topicSubscriber.close();//关闭消费者
                }
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }
}