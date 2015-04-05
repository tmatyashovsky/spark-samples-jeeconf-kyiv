package com.lohika.morning.spark.presentation.spark.driver.reader;

import com.lohika.morning.spark.presentation.spark.driver.context.AnalyticsSparkContext;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.map.FileContentToParticipantsFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.Participant;
import org.apache.spark.api.java.JavaRDD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("rddDataFilesReader")
public class RDDDataFilesReader implements DataFilesReader<JavaRDD<Participant>> {

    @Autowired
    private AnalyticsSparkContext analyticsSparkContext;

    @Override
    public JavaRDD<Participant> readAllParticipants(final boolean includeOnlyPresentParticipants) {
        return getParticipants(includeOnlyPresentParticipants);
    }

    private JavaRDD<Participant> getParticipants(final boolean includeOnlyPresentParticipants) {
        return analyticsSparkContext.readRawData()
                .flatMap(new FileContentToParticipantsFunction(includeOnlyPresentParticipants));
    }

    public AnalyticsSparkContext getAnalyticsSparkContext() {
        return analyticsSparkContext;
    }
}
