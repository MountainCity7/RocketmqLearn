package org.wujiale.rocketmqlearn.test;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class Consumer {

    public static void main(String[] args) throws MQClientException {
        // 1 create a reception message Consumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        // 2 set reception service address
        consumer.setNamesrvAddr("172.16.107.87:9876");
        // 3 set match topic name
        consumer.subscribe("topic1", "*");
        // 4 open listener to receive message
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            /**
             * after set listener, as a message appear, revoke consumeMessage method
             * @param list contain all message
             * @param consumeConcurrentlyContext context for concurrent consumption (multithreading)
             * @return ConsumeConcurrentlyStatus consume status
             */
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt msg : list) {
                    System.out.println("receive message: " + msg);
                    System.out.println("message: " + new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.println("receive service is running");
    }

}
