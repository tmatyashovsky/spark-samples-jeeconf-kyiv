package com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.map;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.EventsByParticipant;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class ToEventsByParticipantFunction implements Function<Tuple2<Long, String>, EventsByParticipant> {

    @Override
    public EventsByParticipant call(Tuple2<Long, String> eventsByParticipant) throws Exception {
        return new EventsByParticipant(eventsByParticipant._2(), eventsByParticipant._1());
    }

}
