package com.slimer.kafka.client.constant;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.util.CollectionUtils;


/**
 * @author Wangbj
 * @since Mar 24, 2016
 */
public class ConfigProp {
    public static final String BOOTSTRAP_SERVER = "bootstrap.servers";
    public static final String KEY_DESERIALIZER = "key.deserializer";
    public static final String KEY_SERIALIZER = "key.serializer";
    public static final String VALUE_DESERIALIZER = "value.deserializer";
    public static final String VALUE_SERIALIZER = "value.serializer";
    public static final String AUTO_OFFSET_RESET = "auto.offset.reset";
    public static final String SESSION_TIMEOUT_MS = "session.timeout.ms";
    public static final String BLOCK_ON_BUFFER_FULL = "block.on.buffer.full";
    public static final String TOPIC = "topic";
    public static final String GROUP = "group.id";
    public static final String REQUEST_TIMEOUT_MS = "request.timeout.ms";
    /*private static Map<Method, Properties> methodPropCache;*/
    private static Map<Method, Properties> consumerPropCache;
    private static Map<Method, Properties> producerPropCache;


    public  static void producerPut(Method method,Properties prop){
        if (null==producerPropCache) {
            producerPropCache=new HashMap<>();
        }
        producerPropCache.put(method,prop);
    }

    public  static Properties producerGet(Method method){
        if (null!=producerPropCache) {
            return producerPropCache.get(method);
        }
        return null;
    }

    public  static void consumerPut(Method method,Properties prop){
        if (null==consumerPropCache) {
            consumerPropCache=new HashMap<>();
        }
        consumerPropCache.put(method,prop);
    }

    public  static Properties consumerGet(Method method){
        if (null!=consumerPropCache) {
            return consumerPropCache.get(method);
        }
        return null;
    }
   /* public static Map<Method, Properties> getConsumerPropCache() {
        if (CollectionUtils.isEmpty(consumerPropCache)) {
            consumerPropCache = new ConcurrentHashMap<Method, Properties>();
        }
        return consumerPropCache;
    }

    public static void setConsumerPropCache(Map<Method, Properties> consumerPropCache) {
        ConfigProp.consumerPropCache = consumerPropCache;
    }

    public static Map<Method, Properties> getProducerPropCache() {
        if (CollectionUtils.isEmpty(producerPropCache)) {
            producerPropCache = new ConcurrentHashMap<Method, Properties>();
        }
        return producerPropCache;
    }

    public static void setProducerPropCache(Map<Method, Properties> producerPropCache) {
        ConfigProp.producerPropCache = producerPropCache;
    }*/
}
