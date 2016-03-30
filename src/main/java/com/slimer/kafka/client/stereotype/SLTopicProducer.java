package com.slimer.kafka.client.stereotype;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Wangbj
 * @since Mar 24, 2016
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SLTopicProducer {
    String topic() default "";
    String topicReference() default "";
    String group() default "";
    int replications() default 2;
    int partitions() default 1;
    String partitionCLass() default "org.apache.kafka.clients.producer.internals.DefaultPartitioner";
    String serializerClass() default "org.apache.kafka.common.serialization.StringDeserializer";
}
