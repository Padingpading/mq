package com.padingpading.mq.rocketmq.transaction;


import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

public class TransactionListenerImpl implements TransactionListener {
    
    /**
     * 完成本地事务逻辑
     *
     * @param message
     * @param o
     *@return org.apache.rocketmq.client.producer.LocalTransactionState
     **/
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        System.out.println("正在执行本地事务----");
        if (StringUtils.equals("TAGA", message.getTags())) {
            //提交消息
            return LocalTransactionState.COMMIT_MESSAGE;
        } else if (StringUtils.equals("TAGB", message.getTags())) {
            //回滚消息
            return LocalTransactionState.ROLLBACK_MESSAGE;
        } else if (StringUtils.equals("TAGC", message.getTags())) {
            //未知
            return LocalTransactionState.UNKNOW;
        }
        return LocalTransactionState.UNKNOW;
    }
    
    /**
     * 消息回查
     *
     * @param messageExt
     *@return org.apache.rocketmq.client.producer.LocalTransactionState
     **/
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        System.out.println("消息的Tag:" + messageExt.getTags());
        //返回事务执行成功
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}

