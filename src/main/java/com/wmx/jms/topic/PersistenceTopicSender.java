package com.wmx.jms.topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import javax.jms.*;
import java.util.UUID;
/**
 * 持久化 topic(主题) 消息生产者
 */
@SuppressWarnings("all")
public class PersistenceTopicSender {
    public static void main(String[] args) {
        Connection connection = null;
        Session session = null;
        MessageProducer messageProducer = null;
        try {
            String brokerURL = "tcp://127.0.0.1:61616";//ActiveMQ 中间件连接地址
            //创建 javax.jms.ConnectionFactory 连接工厂
            ConnectionFactory mqConnectionFactory = new ActiveMQConnectionFactory(brokerURL);
            //如果 ActiveMQ 连不上，则抛异常：java.net.ConnectException: Connection refused: connect
            connection = mqConnectionFactory.createConnection();//通过连接工厂获取连接 javax.jms.Connection
            //创建 session 会话，设置开启事务，消息确认模式为自动确认
            session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
            //创建消息主题(topic)，主题名称自己定义。Queue 、Topic 都是 Destination 的子接口
            Destination destination = session.createTopic("topic-app-2");
            messageProducer = session.createProducer(destination);//根据目的地创建消息生产者
            /**设置消息传递模式为持久化，不写时默认为非持久化。DeliveryMode.NON_PERSISTENT
             * 设置完生产者的传递模式后，再启动连接：connection.start();
             * */
            messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
            connection.start();//启动连接，同理还有 stop、close
            int massageTotal = 5;
            for (int i = 0; i < massageTotal; i++) {
                TextMessage textMessage = session.createTextMessage("密码" + (i + 1) + ":" + UUID.randomUUID());
                messageProducer.send(textMessage);//生产者发送消息
            }
            session.commit();//批量会话提交。此时消息会被正式发送到中间件
            System.out.println("消息发送完毕...");
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
                if (messageProducer != null) {
                    messageProducer.close();//关闭生产者
                }
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }
}