package com.lohika.morning.spark.presentation.spark.distributed.library.function.dataframe.map;

import java.io.Serializable;
import org.apache.spark.sql.Row;
import scala.runtime.AbstractFunction1;

public class ToParticipantEmailFunction extends AbstractFunction1<Row, String> implements Serializable {

    @Override
    public String apply(Row row) {
        return row.getString(0);
    }
}
