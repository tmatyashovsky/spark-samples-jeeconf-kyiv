package com.lohika.morning.spark.presentation.spark.driver.configuration;

import org.apache.spark.SparkConf;

import java.util.Map;

/**
 * A Spring friendly builder to work around overloaded scala bean properties in SparkConf.
 */
public class SparkConfigurationBuilder {

    private final String master;
    private final String appName;
    private final String[] jars;
    private final Map<String, String> sparkProperties;

    public SparkConfigurationBuilder(String master, String appName, String[] jars, Map<String, String> sparkProperties) {
        this.master = master;
        this.appName = appName;
        this.jars = jars;
        this.sparkProperties = sparkProperties;
    }

    public SparkConf buildSparkConfiguration() {
        SparkConf result = new SparkConf()
                .setMaster(master)
                .setAppName(appName == null ? "name-not-set" : appName)
                .setJars(jars);

        // Check if debug mode is on for executors.
        boolean debugMode = false;
        if(sparkProperties.get("spark.executor.isdebugmode") != null) {
            try{
                debugMode = Boolean.parseBoolean(sparkProperties.get("spark.executor.isdebugmode"));
            } catch (Exception e) {
                // Do nothing!
            }
        }

        for (Map.Entry<String, String> e : sparkProperties.entrySet()) {
            // Add the extraJavaOptions only if the debug mode is true.
            if(e.getKey().equals("spark.executor.extraJavaOptions")) {
                if(debugMode) {
                    result.set(e.getKey(), e.getValue());
                }
            } else {
                result.set(e.getKey(), e.getValue());
            }

        }

        return result;
    }
}
