package com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.pair;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.Participant;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class PairParticipantEmailCountFunction implements PairFunction<Participant, String, Long> {

    @Override
    public Tuple2<String, Long> call(Participant participant) throws Exception {
        return new Tuple2<>(participant.getEmail(), 1L);
    }

}
