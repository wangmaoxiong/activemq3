package com.wmx.jms.kahaDB;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.jms.*;
import java.io.IOException;
/**
 * 消息消费者  · 使用非阻塞的监听器方式
 */
@SuppressWarnings("all")
public class ListenerConsumer {
    private static Logger logger = LoggerFactory.getLogger(EmbedActiveMQStart.class);
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
             * 无论是消息队列还是订阅主题 (Topic)，MessageListener 写法都是一样的。
             */
            Destination destination = session.createQueue("queue-app");
            MessageConsumer consumer = session.createConsumer(destination);//创建消息消费者
            /** consumer.receive() 是一个线程阻塞的方法，更加推荐使用异步的消息监听器
             * 为了方便直接使用了匿名内部类，正式开发建议单独作为一个类
             * */
            logger.info("开始监听消息...");
            final Session finalSession = session;
            consumer.setMessageListener(new MessageListener() {
                public void onMessage(Message message) {
                    try {
                        if (message instanceof TextMessage) {
                            TextMessage textMessage = (TextMessage) message;
                            logger.info("接收到消息--> " + textMessage.getText());
                        }
                        finalSession.commit();//消息确认
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            });
            /**
             * 特别提醒：正因为 consumer.setMessageListener 是线程非阻塞的，所以如果不加处理，则 mian 会直接运行结束
             * 这里故意使用 System.in.read() 让 mian 线程阻塞停下来，实际开发中如果是 web 应用，或者是其它的不会马上结束
             * 的 Java SE 应用，则是不需要这样处理的
             */
            System.in.read();//控制台随便输入字符，回车后，程序运行结束
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (IOException e) {
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