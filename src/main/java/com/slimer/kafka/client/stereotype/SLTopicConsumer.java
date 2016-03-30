package com.slimer.kafka.client.stereotype;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.stereotype.Component;

/**
 * @author Wangbj
 * @since Mar 24, 2016
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SLTopicConsumer {
    String topic() default "";
    String topicReference() default "";
    String group() default "";
    int failRetryTimes() default 1;
    int partitionThreadCount() default 1;
    String serializerClass() default "org.apache.kafka.common.serialization.StringDeserializer";
}
