package com.slimer.kafka.client.test;

import java.io.IOException;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slimer.kafka.client.utils.ResourceLoader;

/**
 * @author Wangbj
 * @since Mar 23, 2016
 */
public class KafkaConsumerTest {
    Logger logger = LoggerFactory.getLogger(getClass().getName());
    ObjectMapper objectMapper = new ObjectMapper();
    KafkaConsumer<String, String> consumer;
    ResourceLoader resourceLoader;

    public void consume() {
        resourceLoader = ResourceLoader.getResourceLoader();
        consumer = new KafkaConsumer<String, String>(resourceLoader.load("consumer-config.properties"));
        //消费者订阅topic
        consumer.subscribe(Arrays.asList("fast-messages"));
        int timeouts = 0;
        //从broker循环拉取消息
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(200);
            if (records.count() == 0) {
                timeouts++;
            } else {
                logger.info("get {} records after {} timeouts", records.count(), timeouts);
                timeouts = 0;
            }
            //循环解析获取的值
            for (ConsumerRecord<String, String> consumerRecord : records) {
                switch (consumerRecord.topic()) {
                    case "fast-messages":
                        try {
                            JsonNode jsonNode = objectMapper.readTree(consumerRecord.value());
                            logger.info("receive data:"+jsonNode.toString());
                        } catch (IOException e) {
                            e.printStackTrace();
                            logger.error("consumer error:", e);
                        }
                        break;
                    default:
                        logger.info("receive unIdentifiable topic {}", consumerRecord.topic());
                        break;
                }
            }
        }
    }

    public static void main(String[] args) {
        new KafkaConsumerTest().consume();
    }
}
