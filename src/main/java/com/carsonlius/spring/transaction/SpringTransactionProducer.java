package com.carsonlius.spring.transaction;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Component
public class SpringTransactionProducer {
    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    public void sendMessage(String topic, String message){
        String group = "myTransactionGroup";
        Message msg =  MessageBuilder.withPayload(message).build();


        System.out.println("Message: "+ msg);

        rocketMQTemplate.sendMessageInTransaction(group,topic, msg, "payMoney");
        System.out.println("消息发送成功");
    }
}
