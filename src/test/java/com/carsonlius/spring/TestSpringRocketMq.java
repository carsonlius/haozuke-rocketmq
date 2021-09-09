package com.carsonlius.spring;

import com.carsonlius.spring.transaction.SpringTransactionProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
public class TestSpringRocketMq {
    @Autowired
    private SpringProducer springProducer;

    private final static ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private SpringTransactionProducer springTransactionProducer;

    @Test
    public void testSendMsg(){
        String topic = "spring-my-topic";
        String msg = "第2个spring-rocketMq消息";
        this.springProducer.sendMsg(topic, msg);
        System.out.println("发消息成功");
    }

    @Test
    public void testSendTransaction() throws JsonProcessingException, InterruptedException {

        // 发现消息
        String topic = "pay_topic_spring2:money";
        String tag = "PAY_MESSAGE";

        Map<String, Object> msg = new HashMap<>();
        msg.put("money", 20001);
        msg.put("action", "用户A用户B转钱");

        String  queueMsg= objectMapper.writeValueAsString(msg);
        springTransactionProducer.sendMessage(topic, queueMsg);
        System.out.println("消息发送成功111");
        Thread.sleep(999999L);
    }

}
