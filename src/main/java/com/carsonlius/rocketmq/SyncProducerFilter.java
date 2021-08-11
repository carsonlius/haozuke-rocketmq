package com.carsonlius.rocketmq;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

public class SyncProducerFilter {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("HAOKE_IM");
        defaultMQProducer.setNamesrvAddr("vagrant:9876");
        defaultMQProducer.start();


        String msg = "彭丽霞23";
        Message message = new Message("haokey-meinv-topic", "SEND_MSG", msg.getBytes(StandardCharsets.UTF_8));
        message.putUserProperty("sex", "女");
        message.putUserProperty("age", "22");
//        message.putUserProperty("address", "28");

       SendResult sendResult =  defaultMQProducer.send(message);

        System.out.println("msgId：" + sendResult.getMsgId());
        System.out.println("offsetId:"+ sendResult.getOffsetMsgId());
        System.out.println("status:"+ sendResult.getSendStatus());
        System.out.println("消息queue:" + sendResult.getMessageQueue());
        System.out.println(sendResult);

        defaultMQProducer.shutdown();


    }
}
