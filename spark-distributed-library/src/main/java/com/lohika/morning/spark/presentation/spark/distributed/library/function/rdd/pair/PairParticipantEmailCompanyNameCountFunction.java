package com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.pair;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.Participant;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class PairParticipantEmailCompanyNameCountFunction implements PairFunction<Participant, Tuple2<String, String>, Long> {

    @Override
    public Tuple2<Tuple2<String, String>, Long> call(Participant participant) throws Exception {
        return new Tuple2<>(new Tuple2<>(participant.getEmail(), participant.getCompanyName()), 1L);
    }

}
