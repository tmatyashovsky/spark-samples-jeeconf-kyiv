package com.lohika.morning.spark.presentation.spark.driver.function.rdd;

import com.lohika.morning.spark.presentation.spark.driver.reader.RDDDataFilesReader;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.pair.PairParticipantEmailCountFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.reduce.ReduceBySumValueFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ParticipantsCountFunction {

    @Autowired
    private RDDDataFilesReader dataFilesReader;

    public Long getData(final boolean includeOnlyPresentParticipants) {
        return dataFilesReader.readAllParticipants(includeOnlyPresentParticipants)
                // Removing duplicate emails, avoiding distinct().
                .mapToPair(new PairParticipantEmailCountFunction())
                .reduceByKey(new ReduceBySumValueFunction())
                .count();
    }

}
