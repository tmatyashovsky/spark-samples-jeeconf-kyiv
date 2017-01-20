package com.lohika.morning.spark.presentation.spark.distributed.library.function.dataset.map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

public class ToParticipantEmailFunction implements MapFunction<Row, String> {

    @Override
    public String call(Row row) {
        return row.getString(0);
    }
}
