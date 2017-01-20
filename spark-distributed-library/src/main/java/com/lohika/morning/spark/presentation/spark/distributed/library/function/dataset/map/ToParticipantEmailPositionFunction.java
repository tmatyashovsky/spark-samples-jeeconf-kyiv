package com.lohika.morning.spark.presentation.spark.distributed.library.function.dataset.map;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantEmailPosition;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

public class ToParticipantEmailPositionFunction implements MapFunction<Row, ParticipantEmailPosition> {

    @Override
    public ParticipantEmailPosition call(Row row) {
        return new ParticipantEmailPosition(row.getString(0), row.getString(1));
    }
}
