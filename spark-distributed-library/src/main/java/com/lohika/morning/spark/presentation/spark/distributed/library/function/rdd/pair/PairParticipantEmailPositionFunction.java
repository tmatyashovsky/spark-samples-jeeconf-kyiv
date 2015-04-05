package com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.pair;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.Participant;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantEmailPosition;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class PairParticipantEmailPositionFunction implements PairFunction<Participant, ParticipantEmailPosition, Long> {

    @Override
    public Tuple2<ParticipantEmailPosition, Long> call(Participant participant) throws Exception {
        return new Tuple2<>(new ParticipantEmailPosition(participant.getEmail(), participant.getPosition()), 1L);
    }

}
