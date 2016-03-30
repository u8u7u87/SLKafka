package com.slimer.kafka.client.utils;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.io.Resources;
import com.slimer.kafka.client.constant.ConfigProp;

/**
 * @author Wangbj
 * @since Mar 23, 2016
 */
public class ResourceLoader {
    private static volatile ResourceLoader resourceLoader;
    private Properties properties = new Properties();

    private ResourceLoader() {
    }

    public static ResourceLoader getResourceLoader() {
        if (null == resourceLoader) {
            synchronized (ResourceLoader.class) {
                resourceLoader = new ResourceLoader();
            }
        }
        return resourceLoader;
    }

    public Properties load(String proPath) {
        if (!Strings.isNullOrEmpty(proPath)) {
            try {
                Set<String> propNames = properties.stringPropertyNames();
                if (propNames.size() == 0) {
                    properties.load(Resources.getResource(proPath).openStream());
                } else {
                    Properties temp = new Properties();
                    temp.load(Resources.getResource(proPath).openStream());
                    for (String key : propNames) {
                        temp.setProperty(key, properties.getProperty(key));
                    }
                    properties = temp;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            //// TODO: 3/23/16 log
        }
        return properties;
    }

    public Properties loadConsumerProperties(Properties prop) {
        load("/properties/kafka.properties");
        prop.setProperty(ConfigProp.BOOTSTRAP_SERVER, properties.getProperty(ConfigProp.BOOTSTRAP_SERVER, "localhost:9092"));
        prop.setProperty(ConfigProp.KEY_DESERIALIZER, properties.getProperty(ConfigProp.KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer"));
        prop.setProperty(ConfigProp.VALUE_DESERIALIZER, properties.getProperty(ConfigProp.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer"));
        prop.setProperty(ConfigProp.SESSION_TIMEOUT_MS, properties.getProperty(ConfigProp.SESSION_TIMEOUT_MS, "10000"));
        prop.setProperty(ConfigProp.AUTO_OFFSET_RESET, properties.getProperty(ConfigProp.AUTO_OFFSET_RESET, "latest"));
        prop.setProperty(ConfigProp.REQUEST_TIMEOUT_MS, properties.getProperty(ConfigProp.REQUEST_TIMEOUT_MS, "40000"));
        return prop;
    }

    public Properties loadProducerProperties(Properties prop) {
        load("/properties/kafka.properties");
        prop.setProperty(ConfigProp.BOOTSTRAP_SERVER, properties.getProperty(ConfigProp.BOOTSTRAP_SERVER, "localhost:9092"));
        prop.setProperty(ConfigProp.KEY_DESERIALIZER, properties.getProperty(ConfigProp.KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer"));
        prop.setProperty(ConfigProp.VALUE_DESERIALIZER, properties.getProperty(ConfigProp.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer"));
        return prop;
    }

    public String getValue(String proName) {
        return properties.getProperty(proName);
    }
}
