package com.lohika.morning.spark.presentation.spark.driver.function.rdd;

import com.lohika.morning.spark.presentation.spark.driver.reader.RDDDataFilesReader;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.filter.FilterParticipantByPosition;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.pair.PairParticipantEmailPositionFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantEmailPosition;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ParticipantsByPositionFunction {

    @Autowired
    private RDDDataFilesReader dataFilesReader;

    public List<ParticipantEmailPosition> getData(final String position, final boolean includeOnlyPresentParticipants) {
        return dataFilesReader.readAllParticipants(includeOnlyPresentParticipants)
            .filter(new FilterParticipantByPosition(position))
            .mapToPair(new PairParticipantEmailPositionFunction())
            .groupByKey()
            .sortByKey()
            .keys()
            .collect();
    }

}
