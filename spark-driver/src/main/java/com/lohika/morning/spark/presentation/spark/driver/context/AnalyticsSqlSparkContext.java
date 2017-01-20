package com.lohika.morning.spark.presentation.spark.driver.context;

import com.lohika.morning.spark.presentation.spark.driver.location.HDFSFilesLocation;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AnalyticsSqlSparkContext implements InitializingBean {

    private SQLContext sqlContext;

    @Autowired
    private HDFSFilesLocation dataFilesLocation;

    @Autowired
    private SparkContext sparkContext;

    public SQLContext getSqlContext() {
        return sqlContext;
    }

    public Dataset<Row> readRawData() {
        return sqlContext.parquetFile(this.dataFilesLocation.getPath());
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        sqlContext = new SQLContext(sparkContext);
    }

}
