package com.carsonlius.rocketmq.transaction;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TransactionConsumer {
    public static void main(String[] args) throws MQClientException {
        String group = "transaction-group";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);

        consumer.setNamesrvAddr("vagrant:9876");

        // 发现消息
        String topic = "pay_topic";
        String tag = "PAY_MESSAGE";

        consumer.subscribe(topic, tag);


        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                AtomicBoolean success = new AtomicBoolean(true);
                msgs.forEach(msg->{
                    try{
                        System.out.println(new String(msg.getBody(), StandardCharsets.UTF_8));

                    }catch (Exception e) {
                        e.printStackTrace();
                        success.set(false);
                    }
                });

                if (!success.get()){
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }

                return ConsumeOrderlyStatus.SUCCESS;
            }
        });



        consumer.start();

    }
}
