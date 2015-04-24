package com.lohika.morning.spark.presentation.spark.driver.reader;

import com.lohika.morning.spark.presentation.spark.driver.context.AnalyticsSqlSparkContext;
import org.apache.spark.sql.DataFrame;

public interface DataFrameDataReader extends DataReader<DataFrame> {

    AnalyticsSqlSparkContext getAnalyticsSqlSparkContext();

}
