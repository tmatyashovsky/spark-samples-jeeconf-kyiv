package com.lohika.morning.spark.presentation.spark.driver.function.rdd;

import com.lohika.morning.spark.presentation.spark.driver.reader.RDDDataFilesReader;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.pair.PairParticipantEmailCountFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.reduce.ReduceBySumValueFunction;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ParticipantsEmailsFunction {

    @Autowired
    private RDDDataFilesReader dataFilesReader;

    public List<String> getData(final boolean includeOnlyPresentParticipants) {
        return dataFilesReader.readAllParticipants(includeOnlyPresentParticipants)
                // Removing duplicate emails, avoiding distinct().
                .mapToPair(new PairParticipantEmailCountFunction())
                .reduceByKey(new ReduceBySumValueFunction())
                .sortByKey(true)
                // Collecting unique emails.
                .keys()
                .collect();
    }

}
