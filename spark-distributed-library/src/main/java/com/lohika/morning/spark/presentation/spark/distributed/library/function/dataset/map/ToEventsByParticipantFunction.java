package com.lohika.morning.spark.presentation.spark.distributed.library.function.dataset.map;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.EventsByParticipant;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

public class ToEventsByParticipantFunction implements MapFunction<Row, EventsByParticipant> {

    @Override
    public EventsByParticipant call(Row row) {
        return new EventsByParticipant(row.getString(0), row.getLong(1));
    }

}
