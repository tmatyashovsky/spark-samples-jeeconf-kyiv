package com.lohika.morning.spark.presentation.spark.driver.reader;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.Participant;
import com.lohika.morning.spark.presentation.spark.driver.context.AnalyticsSqlSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("reflectionDataReader")
public class ReflectionDataReader implements DatasetDataReader {

    @Autowired
    private AnalyticsSqlSparkContext analyticsSqlSparkContext;

    @Autowired
    private RDDDataReader rddDataFilesReader;

    public Dataset<Row> readAllParticipants(final boolean includeOnlyPresentParticipants) {
        JavaRDD<Participant> participantsAsRDD = rddDataFilesReader.readAllParticipants(includeOnlyPresentParticipants);

        return toDataFrame(participantsAsRDD);
    }

    private Dataset<Row> toDataFrame(JavaRDD<Participant> participantsAsRDD) {
        // Apply a schema to an RDD of JavaBeans and register it as a table.
        Dataset<Row> participantsAsDataFrames = analyticsSqlSparkContext.getSqlContext()
            .createDataFrame(participantsAsRDD, Participant.class);
        participantsAsDataFrames.registerTempTable("participants");

        return participantsAsDataFrames;
    }

    @Override
    public AnalyticsSqlSparkContext getAnalyticsSqlSparkContext() {
        return analyticsSqlSparkContext;
    }
}
