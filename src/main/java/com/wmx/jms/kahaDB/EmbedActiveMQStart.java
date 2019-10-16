package com.wmx.jms.kahaDB;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * 嵌入式 ActiveMQ 启动
 */
public class EmbedActiveMQStart {
    private static Logger logger = LoggerFactory.getLogger(EmbedActiveMQStart.class);

    /**
     * 嵌入式 ActiveMQ 启动方式 1
     */
    public static void main(String[] args) {
        try {
            /**先创建 KahaDB 持久化适配器对象，指定数据存储目录，同时设置需要设置的属性
             * 同理还有 JDBCPersistenceAdapter、MemoryPersistenceAdapter 等等，它们都是 PersistenceAdapter 接口的实现类
             * 需要什么持久化方式，则创建什么对象即可，实际中通常默认即可
             */
            File dataFileDir = new File("target/kahadb");
            KahaDBStore kahaDBStore = new KahaDBStore();
            kahaDBStore.setDirectory(dataFileDir);
            kahaDBStore.setJournalMaxFileLength(1024 * 100);
            kahaDBStore.setIndexCacheSize(100);

            /**设置 ActiveMQ 消息服务器用于被客户端连接的 url 地址，这里不使用 tcp 传输协议，而是使用 nio 传输协议
             * 实际开发中，地址应该在配置文件中可配置，不要写死
             */
            String serviceURL = "nio://localhost:61616";
            /**BrokerService 表示 ActiveMQ 服务，每一个 BrokerService 表示一个消息服务器实例
             * 如果想启动多个，只需要 start 多个不同端口的 BrokerService 即可*/
            BrokerService brokerService = new BrokerService();
            brokerService.setUseJmx(true);//设置是否应将代理的服务公开到jmx中。默认是 true
            brokerService.setPersistenceAdapter(kahaDBStore);//指定数据持久话适配器
            brokerService.addConnector(serviceURL);//为指定地址添加新的传输连接器
            /**启动 ActiveMQ 服务，此时客户端便可以使用提供的地址进行连接，然后发送消息过来，或者从这里消费消息。
             * 注意：这里内嵌启动后，默认是没有提供 8161 端口的 web 管理界面的，照样能做消息中间件使用
             * */
            brokerService.start();
            logger.info(" ActiveMQ Broker Service Start Finish...");
        } catch (Exception e) {
            logger.error("启动 ActiveMQ BrokerService 失败，cause by:" + e.getMessage(), e);
        }
    }
}