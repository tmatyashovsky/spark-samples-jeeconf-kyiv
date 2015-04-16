package com.lohika.morning.spark.presentation.spark.driver.reader;

import com.lohika.morning.spark.presentation.spark.driver.context.AnalyticsSqlSparkContext;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("parquetDataFilesReader")
public class ParquetDataFilesReader implements DataFrameDataFilesReader {

    @Autowired
    private AnalyticsSqlSparkContext analyticsSqlSparkContext;

    @Override
    public DataFrame readAllParticipants(boolean includeOnlyPresentParticipants) {
        DataFrame participantsAsDataFrames = analyticsSqlSparkContext.readRawData();

        if (includeOnlyPresentParticipants) {
            participantsAsDataFrames = participantsAsDataFrames.filter(participantsAsDataFrames.col("present").equalTo(true));
        }

        // In case we would like to execute traditional SQL.
        participantsAsDataFrames.registerTempTable("participants");

        return participantsAsDataFrames;
    }

    @Override
    public AnalyticsSqlSparkContext getAnalyticsSqlSparkContext() {
        return analyticsSqlSparkContext;
    }
}
