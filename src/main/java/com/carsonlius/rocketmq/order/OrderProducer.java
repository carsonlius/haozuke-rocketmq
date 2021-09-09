package com.carsonlius.rocketmq.order;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class OrderProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        String group = "HAOKE_ORDER_PRODUCER";
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer(group);
        defaultMQProducer.setNamesrvAddr("vagrant:9876");
        defaultMQProducer.start();

        String topic = "haoke_order_topic";
        String tag = "ORDER_MSG";
        for (int i = 0; i < 100; i++) {
            String message = "Order ---->" + i;
            int orderId = i % 10;
            Message msg = new Message(topic, tag, message.getBytes(StandardCharsets.UTF_8));

            defaultMQProducer.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Integer id = (Integer) arg;
                    int size = mqs.size();
                    int index = id % size;
                    System.out.println("msg:" + msg + " arg:" + arg + " index:" + index + " mqsSize:" + size);

                    return mqs.get(index);
                }
            }, orderId);
        }


        defaultMQProducer.shutdown();
    }
}
