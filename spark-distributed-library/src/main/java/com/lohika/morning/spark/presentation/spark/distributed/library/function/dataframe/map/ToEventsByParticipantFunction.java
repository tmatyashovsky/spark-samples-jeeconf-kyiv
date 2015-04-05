package com.lohika.morning.spark.presentation.spark.distributed.library.function.dataframe.map;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.EventsByParticipant;
import java.io.Serializable;
import org.apache.spark.sql.Row;
import scala.runtime.AbstractFunction1;

public class ToEventsByParticipantFunction extends AbstractFunction1<Row, EventsByParticipant> implements Serializable {

    @Override
    public EventsByParticipant apply(Row row) {
        return new EventsByParticipant(row.getString(0), row.getLong(1));
    }

}
