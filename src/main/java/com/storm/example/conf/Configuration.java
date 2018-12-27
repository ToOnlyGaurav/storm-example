package com.storm.example.conf;

import org.apache.storm.Config;

import java.util.Properties;

public class Configuration extends Config {
    public static Configuration fromProperties(Properties properties) {
        Configuration config = new Configuration();

        for (String key : properties.stringPropertyNames()) {
            config.put(key, properties.getProperty(key));
        }
        return config;
    }

}
