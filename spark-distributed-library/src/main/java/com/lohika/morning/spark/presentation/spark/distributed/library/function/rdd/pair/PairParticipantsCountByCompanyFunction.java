package com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.pair;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class PairParticipantsCountByCompanyFunction implements PairFunction<Tuple2<String, Long>, Long, String> {

    @Override
    public Tuple2<Long, String> call(Tuple2<String, Long> companyNameWithParticipantsCount) throws Exception {
        return new Tuple2<>(companyNameWithParticipantsCount._2(), companyNameWithParticipantsCount._1());
    }

}
