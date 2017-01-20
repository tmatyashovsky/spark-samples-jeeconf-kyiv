package com.lohika.morning.spark.presentation.spark.distributed.library.function.dataset.map;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.Participant;
import java.io.Serializable;
import org.apache.spark.sql.Row;
import scala.runtime.AbstractFunction1;

public class ToParticipantFunction extends AbstractFunction1<Row, Participant> implements Serializable {

    @Override
    public Participant apply(Row row) {
        // Pay attention that struct definition will differ depending on programmatic vs. reflection.
        return new Participant(row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4),
            row.getBoolean(5));
    }

}
