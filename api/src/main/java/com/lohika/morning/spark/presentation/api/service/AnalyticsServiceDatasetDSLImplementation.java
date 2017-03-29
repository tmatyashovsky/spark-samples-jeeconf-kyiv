package com.lohika.morning.spark.presentation.api.service;

import com.lohika.morning.spark.presentation.spark.distributed.library.function.dataset.map.ToEventsByParticipantFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.dataset.map.ToParticipantEmailFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.dataset.map.ToParticipantEmailPositionFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.dataset.map.ToParticipantsByCompanyFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.EventsByParticipant;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantEmailPosition;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantsByCompany;
import com.lohika.morning.spark.presentation.spark.driver.reader.DatasetDataReader;
import java.util.List;
import javax.annotation.Resource;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

@Component(value = "analyticsServiceDataFrameDSL")
public class AnalyticsServiceDatasetDSLImplementation implements AnalyticsService {

    @Resource(name = "${datasetDataFilesReader}")
    private DatasetDataReader datasetDataFilesReader;

    @Override
    public List<ParticipantsByCompany> getParticipantsByCompanies(boolean includeOnlyPresentParticipants) {
        Dataset<Row> intermediateDataset = datasetDataFilesReader.readAllParticipants(includeOnlyPresentParticipants)
            // Removing duplicates, one participant from the same company could attend few times.
            .groupBy("email", "companyName")
            .count()
            // Having unique participants, then grouping by company.
            .groupBy("companyName")
            .count();

        return intermediateDataset.orderBy(
                    intermediateDataset.col("count").desc(),
                    intermediateDataset.col("companyName").asc())
                .map(new ToParticipantsByCompanyFunction(), Encoders.bean(ParticipantsByCompany.class))
                .toJavaRDD()
                .collect();
    }

    @Override
    public List<EventsByParticipant> getMostActiveParticipants(boolean includeOnlyPresentParticipants) {
        Dataset<Row> intermediateDataset = datasetDataFilesReader.readAllParticipants(includeOnlyPresentParticipants)
            .groupBy("email")
            .count();

        return intermediateDataset.orderBy(intermediateDataset.col("count").desc(), intermediateDataset.col("email").asc())
            .map(new ToEventsByParticipantFunction(), Encoders.bean(EventsByParticipant.class))
            .toJavaRDD()
            .collect();
    }

    @Override
    public List<ParticipantEmailPosition> getParticipantsByPosition(String position, boolean includeOnlyPresentParticipants) {
        Dataset<Row> allParticipants = datasetDataFilesReader.readAllParticipants(includeOnlyPresentParticipants);
        Column positionColumn = allParticipants.col("position");
        Column emailColumn = allParticipants.col("email");

        return allParticipants.filter(
                positionColumn.contains(position).or(positionColumn.contains(WordUtils.capitalize(position))))
                .groupBy("email", "position")
                .count()
                .orderBy(emailColumn.asc(), positionColumn.asc())
                .map(new ToParticipantEmailPositionFunction(), Encoders.bean(ParticipantEmailPosition.class))
                .toJavaRDD()
                .collect();
    }

    @Override
    public List<String> getUniqueParticipantsEmails(boolean includeOnlyPresentParticipants) {
        return datasetDataFilesReader.readAllParticipants(includeOnlyPresentParticipants)
            // Removing duplicates, one participant from the same company could attend few times.
            .groupBy("email")
            .count()
            .orderBy("email")
            .map(new ToParticipantEmailFunction(), Encoders.STRING())
            .toJavaRDD()
            .collect();
    }

    @Override
    public Long getParticipantsCount(boolean includeOnlyPresentParticipants) {
        return datasetDataFilesReader.readAllParticipants(includeOnlyPresentParticipants)
            .groupBy("email")
            .count()
            .count();
    }

}
