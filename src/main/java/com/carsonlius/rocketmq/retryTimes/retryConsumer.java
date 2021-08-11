package com.carsonlius.rocketmq.retryTimes;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class retryConsumer {
    public static void main(String[] args) throws MQClientException {
        String group = "HAOKE_IM";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamesrvAddr("vagrant:9876");
        String topic = "haoke_im_topic";
        String tag = "SEND_MSG";
        consumer.subscribe(topic, tag);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                AtomicInteger times = new AtomicInteger();

                msgs.forEach(msg->{
                    try {
                        System.out.println("msg:" + new String(msg.getBody()));

                    }catch (Exception e) {
                        e.printStackTrace();

                        if (msg.getReconsumeTimes() > times.get()) {
                            times.set(msg.getReconsumeTimes());
                        }
                    }
                });

                if (times.get() >= 3) {
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
    }
}
