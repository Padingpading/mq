package com.padingpading.mq.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * @author libin
 * @description
 * @date 2022-04-04
 */
public class Consumer {
    
    public static void main(String[] args)
            throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();
        consumer.setNamesrvAddr(RocketMqConfig.server);
        consumer.setConsumerGroup("123");
        //关注的topic
        //消息的过滤器
        MessageSelector messageSelector = MessageSelector.bySql("age>=18 and <=28");
        consumer.subscribe("mytopic","mytopic");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list,
                    ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt messageExt : list) {
                    byte[] body = messageExt.getBody();
                    String s=  new String(body);
                    System.out.println(s);
                }
                //默认消息只会被一个consumer
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.println("consumer start");
    }
    
    public static  void consumerModle() throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();
        consumer.setNamesrvAddr(RocketMqConfig.server);
        consumer.setConsumerGroup("123");
        //消费模式,
        //广播,发送给所有订阅该topic的server
        //consumer.setMessageModel(MessageModel.BROADCASTING);
        //集群,单个consumer消费。同一个group为一个集群。
        consumer.setMessageModel(MessageModel.CLUSTERING);
        
        //关注的topic
        //消息的过滤器
        consumer.subscribe("mytopic","*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list,
                    ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt messageExt : list) {
                    byte[] body = messageExt.getBody();
                    String s=  new String(body);
                    System.out.println(s);
                }
                //默认消息只会被一个consumer
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.println("consumer start");
    }
    
    
}
