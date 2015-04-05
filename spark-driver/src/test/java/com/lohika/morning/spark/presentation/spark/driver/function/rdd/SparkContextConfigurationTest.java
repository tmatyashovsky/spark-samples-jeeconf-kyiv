package com.lohika.morning.spark.presentation.spark.driver.function.rdd;

import com.lohika.morning.spark.presentation.spark.driver.location.DataFilesLocation;
import com.lohika.morning.spark.presentation.spark.driver.location.LocalDataFilesLocation;
import java.net.URL;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:spark-test.properties")
public class SparkContextConfigurationTest {

    @Bean(name = "testDataFilesLocation")
    public DataFilesLocation dataFilesLocation() {
        return new LocalDataFilesLocation() {

            @Override
            public String getPath() {
                URL resource = this.getClass().getResource("/test-data");

                return resource.getPath();
            }

        };
    }

}
