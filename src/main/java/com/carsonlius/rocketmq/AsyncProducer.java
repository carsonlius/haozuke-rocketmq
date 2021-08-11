package com.carsonlius.rocketmq;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

public class AsyncProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("HAOKE_IM");
        defaultMQProducer.setNamesrvAddr("vagrant:9876");

        // 设置失败重发次数
        defaultMQProducer.setRetryTimesWhenSendAsyncFailed(1);

        defaultMQProducer.start();
        String msg = "A发给B的异步消息4";
        String topic = "order";
        String tag = "SEND_MSG";
        Message message = new Message(topic, tag, msg.getBytes(StandardCharsets.UTF_8));


        defaultMQProducer.send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("异步消息发送成功");
                System.out.println(""+ sendResult.getOffsetMsgId());

                System.out.println("消息queue"+ sendResult.getMessageQueue());
                System.out.println("状态"+ sendResult.getSendStatus());
            }

            @Override
            public void onException(Throwable e) {
                System.out.println("发送失败");
                e.printStackTrace();
            }
        });




    }
}
