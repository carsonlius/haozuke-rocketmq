package com.carsonlius.rocketmq.retryTimes;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

public class retrySyncProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        String group = "HAOKE_IM";
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer(group);
        defaultMQProducer.setNamesrvAddr("vagrant:9876");
        defaultMQProducer.setRetryTimesWhenSendFailed(3);
        defaultMQProducer.start();

        String msg = "测试消费者重试";
        String topic = "haoke_im_topic";
        String tag =  "SEND_MSG";
        Message message = new Message(topic,tag, msg.getBytes(StandardCharsets.UTF_8));

        defaultMQProducer.send(message, 1000);


        defaultMQProducer.shutdown();

    }

}
