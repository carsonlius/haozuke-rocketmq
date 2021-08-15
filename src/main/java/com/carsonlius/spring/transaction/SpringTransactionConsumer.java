package com.carsonlius.spring.transaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@RocketMQMessageListener(topic = "pay_topic_spring2", consumerGroup = "haoke-consumer", selectorExpression = "money")
public class SpringTransactionConsumer implements RocketMQListener<String> {
    private final static ObjectMapper object = new ObjectMapper();
    @Override
    public void onMessage(String s) {
        System.out.println("事务消费者" + s);
        JsonNode jsonNode = null;
        try {
            jsonNode = object.readTree(s);
        } catch (IOException e) {
            e.printStackTrace();
        }
        double money = jsonNode.get("money").asDouble();
        if (money == 20001) {
            System.out.println("太多了");
            throw new RuntimeException("money:" + money + "太多了");
        }
    }
}
