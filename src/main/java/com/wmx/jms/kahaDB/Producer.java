package com.wmx.jms.kahaDB;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.UUID;

/**
 * 消息发送者
 */
@SuppressWarnings("all")
public class Producer {
    public static void main(String[] args) {
        Connection connection = null;
        Session session = null;
        try {
            String brokerURL = "nio://127.0.0.1:61616";//ActiveMQ 中间件连接地址,传输协议与服务端要一致
            /**
             * 创建 javax.jms.ConnectionFactory 连接工厂
             * org.apache.activemq.ActiveMQConnectionFactory 中默认设置了大量的参数，还有几个重载的构造器可以选择
             */
            ConnectionFactory mqConnectionFactory = new ActiveMQConnectionFactory(brokerURL);
            //如果 ActiveMQ 连不上，则抛异常：java.net.ConnectException: Connection refused: connect
            connection = mqConnectionFactory.createConnection();//通过连接工厂获取连接 javax.jms.Connection
            connection.start();//启动连接，同理还有 stop、close
            /**Session createSession(boolean transacted, int acknowledgeMode) 创建会话*/
            session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
            /**
             * createQueue(String queueName)：创建消息队列，指定队列名称，消费者可以根据队列名称获取消息
             * Destination 目的地，重点，interface Queue extends Destination
             */
            Destination destination = session.createQueue("queue-app");
            //createProducer(Destination destination)：根据目的地创建消息生产者
            MessageProducer producer = session.createProducer(destination);
            int massageTotal = 5;
            for (int i = 0; i < massageTotal; i++) {
                //创建一个文本消息
                TextMessage textMessage = session.createTextMessage("密钥：" + UUID.randomUUID());
                producer.send(textMessage);//生产者发送消息
                session.commit();//会话提交
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
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }
}