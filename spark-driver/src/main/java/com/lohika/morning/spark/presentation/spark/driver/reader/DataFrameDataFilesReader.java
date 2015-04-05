package com.lohika.morning.spark.presentation.spark.driver.reader;

import com.lohika.morning.spark.presentation.spark.driver.context.AnalyticsSqlSparkContext;
import org.apache.spark.sql.DataFrame;

public interface DataFrameDataFilesReader extends DataFilesReader<DataFrame> {

    AnalyticsSqlSparkContext getAnalyticsSqlSparkContext();

}
