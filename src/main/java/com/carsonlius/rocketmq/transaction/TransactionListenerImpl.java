package com.carsonlius.rocketmq.transaction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class TransactionListenerImpl implements TransactionListener {
    private static Map<String, LocalTransactionState> STATE_MAP = new HashMap<>();

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * When send transactional prepare(half) message succeed, this method will be invoked to execute local transaction.
     *
     * @param msg Half(prepare) message
     * @param arg Custom business parameter
     * @return Transaction state
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {

        System.out.println("事务ID");
        try {
            String message = new String(msg.getBody(), StandardCharsets.UTF_8);
            System.out.println("事务监听器 message:" + message);
            JsonNode jsonNode = objectMapper.readTree(message);
            System.out.println("money" + jsonNode.get("money"));
            int money = jsonNode.get("money").asInt();

            System.out.println("用户A减少" +money+"元");

            Thread.sleep(300);
            System.out.println(1 / 0);

            System.out.println("用户B增加了" +money+"元");
            Thread.sleep(800);
            STATE_MAP.put(msg.getTransactionId(), LocalTransactionState.UNKNOW);

            return LocalTransactionState.COMMIT_MESSAGE;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("事务处理失败");
        }
        STATE_MAP.put(msg.getTransactionId(), LocalTransactionState.COMMIT_MESSAGE);
        return LocalTransactionState.UNKNOW;
    }

    /**
     * When no response to prepare(half) message. broker will send check message to check the transaction status, and this
     * method will be invoked to get local transaction status.
     *
     * @param msg Check message
     * @return Transaction state
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        try {
            System.out.println("回查状态" + msg.getTransactionId() + " msg:" + msg + " STATE_MAP:" + objectMapper.writeValueAsString(STATE_MAP));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return STATE_MAP.get(msg.getTransactionId());
    }
}
