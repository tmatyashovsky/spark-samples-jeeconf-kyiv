package com.lohika.morning.spark.presentation.spark.driver.function.rdd;

import com.lohika.morning.spark.presentation.spark.driver.configuration.SparkContextConfiguration;
import com.lohika.morning.spark.presentation.spark.driver.context.AnalyticsSparkContext;
import org.junit.After;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {SparkContextConfiguration.class, SparkContextConfigurationTest.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public abstract class FunctionTest {

    @Autowired
    private AnalyticsSparkContext analyticsSparkContext;

    @After
    public void tearDown() {
        analyticsSparkContext.getJavaSparkContext().stop();
    }
}
