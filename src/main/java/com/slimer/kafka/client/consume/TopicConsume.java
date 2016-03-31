package com.slimer.kafka.client.consume;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.slimer.kafka.client.constant.ConfigProp;
import com.slimer.kafka.client.utils.ResourceLoader;

/**
 * @author Wangbj
 * @since Mar 24, 2016
 */
public class TopicConsume {
    private static final Logger logger= LoggerFactory.getLogger(TopicConsume.class);
    ExecutorService executorService=null;
    ThreadPoolExecutor threadPoolExecutor=null;
    public void consume(Object object, Method method, Properties prop){
        threadPoolExecutor= (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    ResourceLoader.getResourceLoader().loadConsumerProperties(prop);// 值传递?
                    KafkaConsumer<String,Object> consumer=new KafkaConsumer<String, Object>(prop);
                    consumer.subscribe(Arrays.asList(prop.getProperty(ConfigProp.TOPIC)));
                    while (true){
                        Integer timeout=Integer.parseInt(prop.getProperty(ConfigProp.REQUEST_TIMEOUT_MS));
                        ConsumerRecords<String, Object> records = consumer.poll(timeout);
                        if (records.count()==0) {
                            logger.info("receive nothing from broker.");
                        }else{
                            executorService = Executors.newFixedThreadPool(records.count());
                            //ThreadPoolExecutor threadPoolExecutor= (ThreadPoolExecutor) Executors.newFixedThreadPool(records.count());
                            for (ConsumerRecord<String,Object> record:records) {
                                logger.info("receive data:{}",record.toString());
                                logger.info("begin handle consumer data.");
                                executorService.submit(new TopicConsumeTask(object,method,record));
                                //threadPoolExecutor.execute(new TopicConsumeTask(object,method,record));
                            }
                        }
                    }
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
            }
        });

    }

    public void shutDown(){
        if (null!=executorService) {
            executorService.shutdown();
            threadPoolExecutor.shutdown();
            try {
                executorService.awaitTermination(5, TimeUnit.SECONDS);
                threadPoolExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("shut down error:{}",  e);
            }
        }
    }
}
