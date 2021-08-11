package com.carsonlius.rocketmq.transaction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class TransactionProducer {

    private  static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws MQClientException, InterruptedException, JsonProcessingException {
        String group = "transaction_producer";
        TransactionMQProducer transactionMQProducer = new TransactionMQProducer(group);
        transactionMQProducer.setNamesrvAddr("vagrant:9876");

        // 设置事务监听器
        transactionMQProducer.setTransactionListener(new TransactionListenerImpl());
        transactionMQProducer.start();


        // 发现消息
        String topic = "pay_topic";
        String tag = "PAY_MESSAGE";

        Map<String, Object> msg = new HashMap<>();
        msg.put("money", 1000);
        msg.put("action", "用户A用户B转钱");

        String  queueMsg= objectMapper.writeValueAsString(msg);

        Message message = new Message(topic, tag, queueMsg.getBytes(StandardCharsets.UTF_8));

        TransactionSendResult transactionSendResult = transactionMQProducer.sendMessageInTransaction(message, null);
        System.out.println("queueMsg:" + queueMsg + " getTransactionId:" + transactionSendResult.getTransactionId() + "" + transactionSendResult);


        Thread.sleep(99999999);

        transactionMQProducer.shutdown();


    }

}
