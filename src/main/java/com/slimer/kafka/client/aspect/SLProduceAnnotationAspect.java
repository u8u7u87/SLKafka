package com.slimer.kafka.client.aspect;

import java.lang.reflect.Method;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.slimer.kafka.client.constant.ConfigProp;
import com.slimer.kafka.client.stereotype.SLTopicProducer;
import com.slimer.kafka.client.utils.ResourceLoader;

/**
 * @author Wangbj
 * @since Mar 24, 2016
 */
@Aspect
@Component
public class SLProduceAnnotationAspect {
    private static final Logger logger= LoggerFactory.getLogger(SLProduceAnnotationAspect.class);
    private KafkaProducer<String, Object> kafkaProducer=null;
    @Pointcut(value = "execution(* *(..))")
    public void anyPublicMethod(){}

    @Around("anyPublicMethod() && @annotation(topicProducer)&& args(value)")
    public Object aroundProduce(ProceedingJoinPoint pjp, SLTopicProducer topicProducer,Object value) throws Throwable{
        try {
            MethodSignature signature=(MethodSignature)pjp.getSignature();
            Method method=signature.getMethod();
            logger.info("producer method:{} send data:{} begin.",method,value);
            Properties methodProp= ConfigProp.producerGet(method);
            String topic=methodProp.get("topic").toString();
            ResourceLoader.getResourceLoader().loadProducerProperties(methodProp);// 值传递?
            kafkaProducer=new KafkaProducer<String, Object>(methodProp);
            kafkaProducer.send(new ProducerRecord<String, Object>(topic, value));
            logger.info("producer method:{} send data:{} end.",method,value);
        } catch (Exception e) {
            logger.error("producer send data error:",e);
        }finally {
            kafkaProducer.close();
        }
        return pjp.proceed(new Object[]{value});
    }
}
