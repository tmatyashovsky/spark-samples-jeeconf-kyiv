package com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.map;

import org.apache.commons.lang.WordUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class FileContentToParticipantRowFunction implements Function<String, Row> {

    @Override
    public Row call(String participantRow) throws Exception {
        return toParticipantOutputRow(participantRow);
    }

    // TODO: copy and paste.
    private Row toParticipantOutputRow(String participantInputRow) {
        String[] row = participantInputRow.split(",", -1);

        Object[] participantOutputRow = new Object[6];
        participantOutputRow[0] = "n/a";
        participantOutputRow[1] = "n/a";
        participantOutputRow[2] = "n/a";
        participantOutputRow[3] = "n/a";
        participantOutputRow[4] = "n/a";
        participantOutputRow[5] = false;

        if (!StringUtils.isEmpty(row[0])) {
            participantOutputRow[0] = WordUtils.capitalizeFully(row[0].trim());
        }

        if (!StringUtils.isEmpty(row[1])) {
            participantOutputRow[1] = WordUtils.capitalizeFully(row[1].trim());
        }

        if (!StringUtils.isEmpty(row[2])) {
            participantOutputRow[2] = WordUtils.capitalizeFully(row[2].trim());
        }

        if (!StringUtils.isEmpty(row[3])) {
            participantOutputRow[3] = WordUtils.capitalizeFully(row[3].trim());
        }

        if (!StringUtils.isEmpty(row[4])) {
            participantOutputRow[4] = row[4].trim();
        }

        if (!StringUtils.isEmpty(row[5])) {
            participantOutputRow[5] = "1".equals(row[5].trim());
        }

        return RowFactory.create(participantOutputRow);
    }

}

