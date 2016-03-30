package com.slimer.kafka.client.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.slimer.kafka.client.utils.ResourceLoader;

/**
 * @author Wangbj
 * @since Mar 23, 2016
 */
public class KafkaProducerTest {
    Logger logger = LoggerFactory.getLogger(getClass().getName());
    KafkaProducer<String, String> kafkaProducer;
    ResourceLoader resourceLoader;

    public void produce() {
        resourceLoader = ResourceLoader.getResourceLoader();
        kafkaProducer = new KafkaProducer<String, String>(resourceLoader.load("producer-config.properties"));

        try {
            //循环发送消息
            for (int i = 200; i < 300; i++) {
                String data=String.format("{\"type\":\"test\", \"time\":%.3f, \"index\":%d}", System.nanoTime() * 1e-9, i);
                kafkaProducer.send(new ProducerRecord<String, String>("fast-messages", data));
                logger.info("produce success,data:{}",data);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("producer error:", e);
        } finally {
            kafkaProducer.close();
        }
    }

    public static void main(String[] args) {
        try {
            new KafkaProducerTest().produce();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
