package com.lohika.morning.spark.presentation.spark.distributed.library.function.dataframe.map;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantsByCompany;
import java.io.Serializable;
import scala.runtime.AbstractFunction1;
import org.apache.spark.sql.Row;

public class ToParticipantsByCompanyFunction extends AbstractFunction1<Row, ParticipantsByCompany> implements Serializable {

    @Override
    public ParticipantsByCompany apply(Row row) {
        return new ParticipantsByCompany(row.getString(0), row.getLong(1));
    }

}
