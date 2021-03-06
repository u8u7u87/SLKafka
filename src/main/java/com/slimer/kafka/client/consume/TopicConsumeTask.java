package com.slimer.kafka.client.consume;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.slimer.kafka.client.constant.ConfigProp;
import com.slimer.kafka.client.utils.ResourceLoader;

/**
 * @author Wangbj
 * @since Mar 24, 2016
 */
public class TopicConsumeTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TopicConsumeTask.class);
    private Object object;
    private Method method;
    private ConsumerRecord<String,Object> record;


    public TopicConsumeTask(Object object, Method method, ConsumerRecord<String,Object> record) {
        this.object = object;
        this.method = method;
        this.record = record;
    }

    @Override
    public void run() {
        try {
            logger.info("method :{} begin invoke consumer,data:{}",method,record);
            method.invoke(object,record.value());
            logger.info("method :{} end invoke consumer,data:{}",method,record);
        } catch (IllegalAccessException e) {
            logger.error("invoke error:",e);
        } catch (IllegalArgumentException e) {
            logger.error("invoke error:",e);
        }  catch (InvocationTargetException e) {
            logger.error("invoke error:",e);
        }catch (RuntimeException e) {
            logger.error("invoke error:",e);
        }
    }
}
