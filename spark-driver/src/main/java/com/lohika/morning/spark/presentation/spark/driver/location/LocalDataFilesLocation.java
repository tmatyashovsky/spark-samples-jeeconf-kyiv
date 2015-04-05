package com.lohika.morning.spark.presentation.spark.driver.location;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component("localDataFilesLocation")
public class LocalDataFilesLocation implements DataFilesLocation {

    @Value("${spark.path-to-local-data}")
    private String path;

    @Override
    public String getPath() {
        return path;
    }

}
