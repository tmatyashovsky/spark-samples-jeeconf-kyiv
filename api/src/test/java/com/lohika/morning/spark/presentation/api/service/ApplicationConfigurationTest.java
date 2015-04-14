package com.lohika.morning.spark.presentation.api.service;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource({"classpath:spark-test.properties", "classpath:application-test.properties", })
public class ApplicationConfigurationTest {}
