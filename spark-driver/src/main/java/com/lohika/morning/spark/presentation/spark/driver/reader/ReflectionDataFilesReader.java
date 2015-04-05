package com.lohika.morning.spark.presentation.spark.driver.reader;

import com.lohika.morning.spark.presentation.spark.driver.context.AnalyticsSqlSparkContext;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.Participant;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("reflectionDataFilesReader")
public class ReflectionDataFilesReader implements DataFrameDataFilesReader {

    @Autowired
    private AnalyticsSqlSparkContext analyticsSqlSparkContext;

    @Autowired
    private RDDDataFilesReader rddDataFilesReader;

    public DataFrame readAllParticipants(final boolean includeOnlyPresentParticipants) {
        JavaRDD<Participant> participantsAsRDD = rddDataFilesReader.readAllParticipants(includeOnlyPresentParticipants);

        return toDataFrame(participantsAsRDD);
    }

    private DataFrame toDataFrame(JavaRDD<Participant> participantsAsRDD) {
        // Apply a schema to an RDD of JavaBeans and register it as a table.
        DataFrame participantsAsDataFrames = analyticsSqlSparkContext.getSqlContext()
            .createDataFrame(participantsAsRDD, Participant.class);
        participantsAsDataFrames.registerTempTable("participants");

        return participantsAsDataFrames;
    }

    @Override
    public AnalyticsSqlSparkContext getAnalyticsSqlSparkContext() {
        return analyticsSqlSparkContext;
    }
}
