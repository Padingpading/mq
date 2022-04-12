package com.padingpading.mq.rocketmq;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author libin
 * @description
 * @date 2022-04-04
 */
public class Producer {
    
   private static DefaultMQProducer producer = null;
    
   static {
        producer = new DefaultMQProducer("mygroup");
        producer.setNamesrvAddr(RocketMqConfig.server);
        producer.setVipChannelEnabled(false);
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args)
            throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        System.out.println("ddfsdf");
        //asuyc();
        //单向发送
        //oneWay();
        tag();
    }

    
    public void suyc() throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
            Message message = new Message("mytopic","测11试内容".getBytes(StandardCharsets.UTF_8));
            producer.send(message);
            producer.shutdown();
    }
    
    public static void asuyc() throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("mygroup");
        Message message = new Message("mytopic","测11试内容".getBytes(StandardCharsets.UTF_8));
        producer.send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println(sendResult);
            }
    
            @Override
            public void onException(Throwable throwable) {
                //发生异常
                throwable.printStackTrace();
            }
        });
    }
    
    public static void oneWay() throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("mygroup");
        Message message = new Message("mytopic","测11试内容".getBytes(StandardCharsets.UTF_8));
        producer.sendOneway(message);
    }
    
    public void sendBatch() throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        Message message = new Message("mytopic","第一条".getBytes(StandardCharsets.UTF_8));
        Message message1 = new Message("mytopic","第二条".getBytes(StandardCharsets.UTF_8));
        Message message2 = new Message("mytopic","第三条".getBytes(StandardCharsets.UTF_8));
        List<Message> ara = new ArrayList<>();
        ara.add(message);
        ara.add(message1);
        ara.add(message2);
        producer.send(ara);
        producer.shutdown();
    }
    
    public static void tag() throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        Message message = new Message("mytopic","mytag","tag测试".getBytes(StandardCharsets.UTF_8));
        producer.sendOneway(message);
        producer.shutdown();
    }
    
    
    public void sendProperty() throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        Message message = new Message("mytopic","第一条".getBytes(StandardCharsets.UTF_8));
        message.putUserProperty("age","18");
        producer.send(message);
        producer.shutdown();
    }
}
