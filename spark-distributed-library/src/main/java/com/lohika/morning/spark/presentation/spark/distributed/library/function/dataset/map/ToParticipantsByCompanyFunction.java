package com.lohika.morning.spark.presentation.spark.distributed.library.function.dataset.map;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantsByCompany;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

public class ToParticipantsByCompanyFunction implements MapFunction<Row, ParticipantsByCompany> {

    @Override
    public ParticipantsByCompany call(Row row) {
        return new ParticipantsByCompany(row.getString(0), row.getLong(1));
    }

}
