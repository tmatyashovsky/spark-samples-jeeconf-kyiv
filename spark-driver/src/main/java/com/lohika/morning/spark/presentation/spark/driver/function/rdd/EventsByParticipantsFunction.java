package com.lohika.morning.spark.presentation.spark.driver.function.rdd;

import com.lohika.morning.spark.presentation.spark.driver.reader.RDDDataFilesReader;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.map.ToEventsByParticipantFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.pair.PairEventsCountByParticipantFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.pair.PairParticipantEmailCountFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.reduce.ReduceBySumValueFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.EventsByParticipant;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class EventsByParticipantsFunction {

    @Autowired
    private RDDDataFilesReader dataFilesReader;

    public List<EventsByParticipant> getData(final boolean includeOnlyPresentParticipants) {
        return dataFilesReader.readAllParticipants(includeOnlyPresentParticipants)
                    .mapToPair(new PairParticipantEmailCountFunction())
                    .reduceByKey(new ReduceBySumValueFunction())
                    .sortByKey()
                    .mapToPair(new PairEventsCountByParticipantFunction())
                    .sortByKey(false)
                    .map(new ToEventsByParticipantFunction())
                    .collect();
    }

}
