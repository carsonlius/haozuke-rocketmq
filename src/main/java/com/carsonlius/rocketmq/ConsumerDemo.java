package com.carsonlius.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ConsumerDemo {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("HAOKE_IM");
        defaultMQPushConsumer.setNamesrvAddr("vagrant:9876");

        defaultMQPushConsumer.subscribe("order", "SEND_MSG || add || TagA");

        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                msgs.forEach(msg -> {
                    try {
                        String message = new String(msg.getBody(), StandardCharsets.UTF_8);
                        System.out.println("消息内容:" + message);
                        System.out.println("消息内容:" + msg);

                    } catch (Exception e) {
                        System.out.println("消费遇到问题");
                        e.printStackTrace();
                    }
                });

                System.out.println("收到了消息:" + context);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        defaultMQPushConsumer.start();

    }
}
