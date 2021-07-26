package com.carsonlius.rocketmq;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

public class SyncProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {

        DefaultMQProducer producer = new DefaultMQProducer("test-group");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        for (int i = 0; i < 100; i++) {
            String topic = "order";
            String tag = "TagA";
            byte[] body = ("Hello RocketMq" + i).getBytes(StandardCharsets.UTF_8);

            Message message = new Message(topic,tag, body);
            SendResult sendResult = producer.send(message);
            System.out.println("sendResult:"+ sendResult);
        }

        System.out.println("关闭消费者");
        producer.shutdown();
    }
}
