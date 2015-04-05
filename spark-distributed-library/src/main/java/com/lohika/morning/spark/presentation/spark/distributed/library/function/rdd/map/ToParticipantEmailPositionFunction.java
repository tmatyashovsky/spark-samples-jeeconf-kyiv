package com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.map;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantEmailPosition;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class ToParticipantEmailPositionFunction implements Function<Tuple2<String, String>, ParticipantEmailPosition> {

    @Override
    public ParticipantEmailPosition call(Tuple2<String, String> emailAndPosition) throws Exception {
        return new ParticipantEmailPosition(emailAndPosition._1(), emailAndPosition._2());
    }

}
