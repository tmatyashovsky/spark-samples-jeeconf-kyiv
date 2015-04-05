package com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.pair;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


public class PairEventsCountByParticipantFunction implements PairFunction<Tuple2<String, Long>, Long, String> {

    @Override
    public Tuple2<Long, String> call(Tuple2<String, Long> participantWithEventsCount) throws Exception {
        return new Tuple2<>(participantWithEventsCount._2(), participantWithEventsCount._1());
    }

}
