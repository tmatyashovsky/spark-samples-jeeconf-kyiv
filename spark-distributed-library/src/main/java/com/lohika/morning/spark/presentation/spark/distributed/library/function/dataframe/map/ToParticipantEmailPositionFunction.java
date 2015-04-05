package com.lohika.morning.spark.presentation.spark.distributed.library.function.dataframe.map;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantEmailPosition;
import java.io.Serializable;
import org.apache.spark.sql.Row;
import scala.runtime.AbstractFunction1;

public class ToParticipantEmailPositionFunction extends AbstractFunction1<Row, ParticipantEmailPosition> implements Serializable {

    @Override
    public ParticipantEmailPosition apply(Row row) {
        return new ParticipantEmailPosition(row.getString(0), row.getString(1));
    }
}
