package com.carsonlius.spring.transaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.messaging.Message;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@RocketMQTransactionListener(txProducerGroup = "myTransactionGroup")
public class TransactionListenerImpl implements RocketMQLocalTransactionListener {
    private static final Map<String, RocketMQLocalTransactionState> STATE_MAP = new HashMap<>();

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message msg, Object o) {

        String transId = (String) msg.getHeaders().get(RocketMQHeaders.TRANSACTION_ID);
        System.out.println("事务ID" + transId);

        try {
            String message =new String((byte[]) msg.getPayload(), StandardCharsets.UTF_8);

            JsonNode jsonNode = objectMapper.readTree(message);
            System.out.println("money" + jsonNode.get("money"));
            int money = jsonNode.get("money").asInt();

            System.out.println("用户A减少" +money+"元");

            Thread.sleep(300);

            System.out.println("用户B增加了" +money+"元");
            Thread.sleep(800);
            STATE_MAP.put(transId, RocketMQLocalTransactionState.COMMIT);

            return RocketMQLocalTransactionState.COMMIT;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("事务处理失败");
        }
        STATE_MAP.put(transId, RocketMQLocalTransactionState.ROLLBACK);
        return RocketMQLocalTransactionState.ROLLBACK;
    }

    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message message) {

        String transId = (String) message.getHeaders().get(RocketMQHeaders.TRANSACTION_ID);
        String msg =new String((byte[]) message.getPayload(), StandardCharsets.UTF_8);
        System.out.println("事务回查" + transId + " message:" + msg + "事务结果" + STATE_MAP.get(transId));
        return STATE_MAP.get(transId);
    }
}
