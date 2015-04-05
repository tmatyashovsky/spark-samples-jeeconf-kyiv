package com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.map;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class FileContentToFileNameFunction implements Function<Tuple2<String, String>, String> {

    @Override
    public String call(Tuple2<String, String> filenameAndContent) throws Exception {
        return filenameAndContent._1();
    }

}

