package com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.map;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantsByCompany;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class ToParticipantsByCompanyFunction implements Function<Tuple2<Long, String>, ParticipantsByCompany> {

    @Override
    public ParticipantsByCompany call(Tuple2<Long, String> participantsCountByCompanyName) throws Exception {
        return new ParticipantsByCompany(participantsCountByCompanyName._2(),
                participantsCountByCompanyName._1());
    }

}
