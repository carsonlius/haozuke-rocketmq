package com.carsonlius.rocketmq;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.sql.Time;

public class SyncProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {

        DefaultMQProducer producer = new DefaultMQProducer("HAOKE_IM");
        producer.setNamesrvAddr("192.168.56.11:9876");
        producer.start();
        String topic = "order";

         producer.createTopic("broker_haoke_im",topic, 4);
        for (int i = 0; i < 100000000 ; i++) {
            String tag = "TagA";
            byte[] body = ("Hello RocketMq" + i).getBytes(StandardCharsets.UTF_8);

            Message message = new Message(topic,tag, body);
            SendResult sendResult = producer.send(message);
//            System.out.println("sendResult:"+ sendResult);
            System.out.println( "第" + i + "个");
            if (i%10000 == 0 && i != 0) {
                Thread.sleep(1000);
            }
        }

        System.out.println("关闭消费者");
        producer.shutdown();
    }
}
