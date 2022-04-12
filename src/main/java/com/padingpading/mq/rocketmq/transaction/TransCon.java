package com.padingpading.mq.rocketmq.transaction;

import com.padingpading.mq.rocketmq.RocketMqConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author libin
 * @description
 * @date 2022-04-04
 */
public class TransCon {
    
    public static void main(String[] args) throws Exception {
        // 1.创建消费者Consumer，制定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("demo_transaction_group");
        // 2.指定Nameserver地址
        consumer.setNamesrvAddr(RocketMqConfig.server);
        // 3.订阅主题Topic和Tag
        consumer.subscribe("TransactionTopic", "*");
        
        // 4.设置回调函数，处理消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            
            // 接受消息内容
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println(
                            "consumeThread=" + Thread.currentThread().getName() + "," + new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 5.启动消费者consumer
        consumer.start();
        System.out.println("消费者启动");
    }
}
