package com.lohika.morning.spark.presentation.spark.driver.context;

import com.lohika.morning.spark.presentation.spark.driver.location.DataFilesLocation;
import javax.annotation.Resource;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AnalyticsSparkContext implements InitializingBean {

    private JavaSparkContext javaSparkContext;

    @Autowired
    private SparkContext sparkContext;

    @Resource(name = "${dataFilesLocation}")
    private DataFilesLocation dataFilesLocation;

    public JavaPairRDD<String, String> readRawData() {
        return javaSparkContext.wholeTextFiles(dataFilesLocation.getPath());
    }

    public JavaSparkContext getJavaSparkContext() {
        return javaSparkContext;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        javaSparkContext = new JavaSparkContext(sparkContext);
    }
}
