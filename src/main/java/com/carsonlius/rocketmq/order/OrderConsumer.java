package com.carsonlius.rocketmq.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class OrderConsumer {
    public static void main(String[] args) throws MQClientException {
        String group = "HAOKE_ORDER_PRODUCER";

        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer(group);
        defaultMQPushConsumer.setNamesrvAddr("vagrant:9876");
        String topic = "haoke_order_topic";
        String tag = "ORDER_MSG";

        defaultMQPushConsumer.subscribe(topic, tag);
        defaultMQPushConsumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                msgs.forEach(msg->{
                    System.out.println(Thread.currentThread().getName() + " transactionId:" + msg.getTransactionId() + " queueId:" + msg.getQueueId() + " msg:" + new String(msg.getBody(), StandardCharsets.UTF_8));
                });


                return ConsumeOrderlyStatus.SUCCESS;
            }
        });





        defaultMQPushConsumer.start();
    }
}
