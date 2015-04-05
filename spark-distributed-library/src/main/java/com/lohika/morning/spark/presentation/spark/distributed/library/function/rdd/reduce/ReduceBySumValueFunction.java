package com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.reduce;

import org.apache.spark.api.java.function.Function2;

public class ReduceBySumValueFunction implements Function2<Long, Long, Long> {

    @Override
    public Long call(Long x, Long y) throws Exception {
        return x + y;
    }

}
