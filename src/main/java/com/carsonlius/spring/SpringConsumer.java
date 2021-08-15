package com.carsonlius.spring;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@RocketMQMessageListener(topic = "spring-my-topic", consumerGroup = "haokey-consumer", selectorExpression = "*")
public class SpringConsumer implements RocketMQListener<String> {


    @Override
    public void onMessage(String s) {



        System.out.println("消费者接收到消息" + s);
    }
}
