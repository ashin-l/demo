package com.example.demo.service.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import javax.swing.*;

import com.example.demo.config.KafkaConfig;
import com.example.demo.controller.WebsocketServer;
import com.example.demo.util.SpringUtil;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerRunner implements Runnable {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer consumer;
    // private final String topic;
    // private Object KafkaConfig;
    private KafkaConfig conf = (KafkaConfig) SpringUtil.getBean("kafkaConfig");

    public static final String BOOTSTRAP_SERVER = "k8s-node-1:9092";
    public static final String CLIENT_ID = "SampleConsumer";

    public KafkaConsumerRunner() {
        System.out.println(conf.getVehicle());
        Properties props = new Properties();
        // 配置kafka集群机器
        props.put("bootstrap.servers", conf.getBootstrapServers());
        // props.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        // 消费者分组
        props.put("group.id", conf.getGroupId());
        // 这里设置 消费者自动提交已消费消息的offset
        props.put("enable.auto.commit", false);
        // 设置自动提交的时间间隔为1000毫秒
        // props.put("auto.commit.interval.ms", "1000");
        // 设置每次poll的最大数据个数
        props.put("max.poll.records", 5);
        // 设置使用最开始的offset偏移量为该group.id的最早。如果不设置，则会是latest即该topic最新一个消息的offset
        // 如果采用latest，消费者只能得道其启动后，生产者生产的消息
        // 一般配置earliest 或者latest 值
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);
    }

    public void run() {
        // closed.set(false);
        try {
            consumer.subscribe(Arrays.asList(conf.getVehicle()));
            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                // Handle new records
                for (ConsumerRecord record : records) {
                    System.out.println(record.value());
                    WebsocketServer.sendAll(record.value().toString());
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get())
                throw e;
        } finally {
            consumer.close();
        }
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
