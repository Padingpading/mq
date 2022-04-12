package com.padingpading.mq.rocketmq.transaction;



import com.padingpading.mq.rocketmq.RocketMqConfig;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author libin
 * @description 发送事务消息
 * @date 2022-04-04
 */
public class CheckLocalProducer {
    
    public static void main(String[] args) throws Exception {
        // 1.创建消息生产者producer，并制定生产者组名
        TransactionMQProducer producer = new TransactionMQProducer("demo_transaction_group");
        // 2.指定Nameserver地址
        producer.setNamesrvAddr(RocketMqConfig.server);
        
        // 添加事务监听器
        TransactionListenerImpl transactionListener = new TransactionListenerImpl();
        producer.setTransactionListener(transactionListener);
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });
        // 设置线程池
        producer.setExecutorService(executorService);
        // 3.启动producer
        producer.start();
        System.out.println("生产者启动");
        
        String[] tags = { "TAGC", "TAGA", "TAGB" };
        
        for (int i = 0; i < 3; i++) {
            // 4.创建消息对象，指定主题Topic、Tag和消息体
            /**
             * 参数一：消息主题Topic
             * 参数二：消息Tag
             * 参数三：消息内容
             */
            Message msg = new Message("TransactionTopic", tags[i], ("Hello xuzhu" + i).getBytes());
            // 5.发送消息
            SendResult result = producer.sendMessageInTransaction(msg, "hello-xuzhu_transaction");
            // 发送状态
            SendStatus status = result.getSendStatus();
            
            System.out.println("发送结果:" + result);
            System.out.println("发送结果状态:" + status);
            // 线程睡120秒
            TimeUnit.SECONDS.sleep(120);
        }
        
        // 6.关闭生产者producer
        producer.shutdown();
        System.out.println("生产者结束");
    }
}
