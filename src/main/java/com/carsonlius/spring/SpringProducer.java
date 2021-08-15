package com.carsonlius.spring;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SpringProducer {
    // 注入rocketmq模板
    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    public void sendMsg(String topic, String msg){

//        String topic = "spring-my-topic";
//        String msg = "第一个spring-rocketMq消息";

        this.rocketMQTemplate.convertAndSend(topic, msg);
    }

}
