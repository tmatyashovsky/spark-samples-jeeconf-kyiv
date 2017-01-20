package com.lohika.morning.spark.presentation.spark.driver.reader;

import com.lohika.morning.spark.presentation.spark.driver.context.AnalyticsSqlSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface DatasetDataReader extends DataReader<Dataset<Row>> {

    AnalyticsSqlSparkContext getAnalyticsSqlSparkContext();

}
