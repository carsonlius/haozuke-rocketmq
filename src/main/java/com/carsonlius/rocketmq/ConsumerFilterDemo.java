package com.carsonlius.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;

public class ConsumerFilterDemo{
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("HAOKE_IM");
        consumer.setNamesrvAddr("vagrant:9876");
        String topic = "haokey-meinv-topic";

        consumer.subscribe(topic, MessageSelector.bySql("age >=18 AND sex='女'"));

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach(msg->{
                System.out.println("消息内容:"+ new String(msg.getBody()));
                System.out.println("age" + msg.getProperty("age"));
                System.out.println("sex" + msg.getProperty("sex"));
                System.out.println(msg);
                System.out.println("");
            });

            System.out.println("接收到消息");
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });



        consumer.start();
    }
}
