package com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.pair;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class PairParticipantCompanyNameCountFunction implements PairFunction<Tuple2<String, String>, String, Long> {

    @Override
    public Tuple2<String, Long> call(Tuple2<String, String> emailWithCompanyName) throws Exception {
        return new Tuple2<>(emailWithCompanyName._2(), 1L);
    }

}
