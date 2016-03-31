package com.slimer.kafka.client;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.io.InputStreamResource;
import org.springframework.util.CollectionUtils;

import com.google.common.base.Strings;
import com.google.common.io.Resources;
import com.slimer.kafka.client.constant.ConfigProp;
import com.slimer.kafka.client.consume.TopicConsume;
import com.slimer.kafka.client.stereotype.SLMessageService;
import com.slimer.kafka.client.stereotype.SLTopicConsumer;
import com.slimer.kafka.client.stereotype.SLTopicProducer;

/**
 * @author Wangbj
 * @since Mar 25, 2016
 */
public class SLApplicationContext implements DisposableBean, ApplicationContextAware {
    private static final Logger logger = LoggerFactory.getLogger(SLApplicationContext.class);
    private List<TopicConsume> consumeList=new ArrayList<>();
    private ApplicationContext applicationContext = null;

    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    @PostConstruct
    public void init() {
        doPackageScan();
    }

    private void doPackageScan() {
        try {
            Map<String, Object> beanMap = this.getApplicationContext().getBeansWithAnnotation(SLMessageService.class);
            handleScanClass(beanMap);
        } catch (Exception e) {
            logger.error("package scan error:", e);
        }

    }

    private void handleScanClass(Map<String, Object> beanMap) {
        for (String key:beanMap.keySet()){
            Object o=beanMap.get(key);

            Method[] methods = AopUtils.getTargetClass(o).getDeclaredMethods();
            for (Method method : methods) {
                Annotation[] annotations = method.getAnnotations();
                for (Annotation annotation : annotations) {
                    if (annotation instanceof SLTopicConsumer) {
                        handleConsumerClass(o, method, (SLTopicConsumer) annotation);
                    }
                    if (annotation instanceof SLTopicProducer) {
                        handleProducerClass(o, method, (SLTopicProducer) annotation);
                    }
                }
            }
        }
    }

    private void handleConsumerClass(Object object, Method method, SLTopicConsumer sltc) {
        Properties prop = loadConsumerProperties(sltc);
        ConfigProp.consumerPut(method, prop);
        TopicConsume topicConsume=new TopicConsume();
        topicConsume.consume(object, method, prop);
        consumeList.add(topicConsume);
    }

    private void handleProducerClass(Object object, Method method, SLTopicProducer sltp) {
        Properties prop = loadProducerProperties(sltp);
        ConfigProp.producerPut(method, prop);
    }

    private Properties loadConsumerProperties(SLTopicConsumer sltc) {
        Properties prop = new Properties();
        //get data from reference config is not null
        if (!Strings.isNullOrEmpty(sltc.topicReference())) {
            prop = loadProperties(sltc.topicReference().trim());
        } else if (!Strings.isNullOrEmpty(sltc.topic())) {
            prop.setProperty(ConfigProp.TOPIC, sltc.topic());
            prop.setProperty(ConfigProp.GROUP, sltc.group());
        }
        return prop;

    }

    private Properties loadProducerProperties(SLTopicProducer sltp) {
        Properties prop = new Properties();
        //get data from reference config is not null
        if (!Strings.isNullOrEmpty(sltp.topicReference())) {
            prop = loadProperties(sltp.topicReference().trim());
        } else if (!Strings.isNullOrEmpty(sltp.topic())) {
            prop.setProperty(ConfigProp.TOPIC, sltp.topic());
            prop.setProperty(ConfigProp.GROUP, sltp.group());
        }
        return prop;
    }

    private Properties loadProperties(String reference) {
        Properties prop = new Properties();
        try {
            //test.topic1
            Properties properties = new Properties();
            String fileName = reference.substring(0, reference.lastIndexOf(".") + 1) + ".properties";
            String topicProperty = reference.substring(reference.lastIndexOf(".") + 1).trim();
            logger.info("load reference,topic file path:{},topic name:{}", fileName, topicProperty);
            properties.load(this.getClass().getResourceAsStream("/properties/" + fileName));
            String formatString = properties.getProperty(topicProperty);
            if (Strings.isNullOrEmpty(formatString)) {
                logger.error("loadProperties doesn't contain the key.");
                return null;
            }
            String[] confItems = formatString.split(",");

            if (null != confItems && confItems.length > 0) {
                for (String s : confItems) {
                    String[] kv = s.split(":");
                    if (null != kv && kv.length == 2) {
                        if (kv[0].isEmpty() || kv[1].isEmpty()) {
                            logger.error("loadProperties key and value cann't be null.");
                            return null;
                        }
                        prop.setProperty(kv[0], kv[1]);
                    }
                }
            } else {
                logger.error("loadProperties key-value data error.");
                return null;
            }
        } catch (Exception e) {
            logger.error("loadProperties error:{}", e);
        }
        return prop;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void destroy() throws Exception {
        for (TopicConsume topicConsume : consumeList) {
            topicConsume.shutDown();
        }
    }
}
