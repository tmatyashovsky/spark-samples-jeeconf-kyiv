package com.lohika.morning.spark.presentation.spark.driver.location;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component("hdfsDataFilesLocation")
public class HDFSFilesLocation implements DataFilesLocation {

    @Value("${spark.path-to-hdfs-data}")
    private String path;

    @Override
    public String getPath() {
        return path;
    }

}
