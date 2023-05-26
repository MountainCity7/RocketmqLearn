package org.wujiale.rocketmqlearn.test;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;


public class Producer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException, UnsupportedEncodingException {
        //1.创建一个发送消息的对象Producer，指定分组（生产者分组）  等会讲
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //2.设定发送的命名服务器地址，连接上ns之后，才能拿到broker地址，发送消息
        producer.setNamesrvAddr("172.16.107.87:9876");
        //3.1启动发送的服务
        producer.start();
        //4.创建要发送的消息对象,指定topic，指定内容body
        Message msg = new Message("topic1","hello consumer".getBytes(StandardCharsets.UTF_8));
        //3.2发送消息。这里是同步请求，如果broker没有给出响应，就拿不到返回值并且卡死在当前行代码
        SendResult result = producer.send(msg);
        System.out.println("返回结果："+result);
        //5.关闭连接
        producer.shutdown();
    }
}
