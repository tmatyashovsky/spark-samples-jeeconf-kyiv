package com.lohika.morning.spark.presentation.spark.driver.reader;

import com.lohika.morning.spark.presentation.spark.driver.context.AnalyticsSqlSparkContext;
import java.util.HashMap;
import javax.annotation.Resource;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("postgresDataReader")
public class PostgresDataReader implements DataFrameDataReader {

    @Resource(name = "jdbcOptions")
    private HashMap<String, String> jdbcOptions;

    @Autowired
    private AnalyticsSqlSparkContext analyticsSqlSparkContext;

    @Override
    public AnalyticsSqlSparkContext getAnalyticsSqlSparkContext() {
        return analyticsSqlSparkContext;
    }

    @Override
    public DataFrame readAllParticipants(boolean includeOnlyPresentParticipants) {
        // Issue with column names - lower case vs. camel case.
        DataFrame participantsAsDataFrames = analyticsSqlSparkContext.getSqlContext().load("jdbc", jdbcOptions);

        // In case we would like to execute traditional SQL.
        participantsAsDataFrames.registerTempTable("participants");

        return participantsAsDataFrames;
    }

}
