package com.lohika.morning.spark.presentation.api.service;

import com.lohika.morning.spark.presentation.spark.distributed.library.function.dataframe.map.ToEventsByParticipantFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.dataframe.map.ToParticipantEmailFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.dataframe.map.ToParticipantEmailPositionFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.dataframe.map.ToParticipantsByCompanyFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.EventsByParticipant;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantEmailPosition;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantsByCompany;
import com.lohika.morning.spark.presentation.spark.driver.reader.DataFrameDataReader;
import java.util.List;
import javax.annotation.Resource;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.springframework.stereotype.Component;
import scala.reflect.ClassTag$;

@Component(value = "analyticsServiceDataFrameDSL")
public class AnalyticsServiceDataFrameDSLImplementation implements AnalyticsService {

    @Resource(name = "${dataFrameDataFilesReader}")
    private DataFrameDataReader dataFrameDataFilesReader;

    @Override
    public List<ParticipantsByCompany> getParticipantsByCompanies(boolean includeOnlyPresentParticipants) {
        DataFrame intermediateDataFrame = dataFrameDataFilesReader.readAllParticipants(includeOnlyPresentParticipants)
            // Removing duplicates, one participant from the same company could attend few times.
            .groupBy("email", "companyName")
            .count()
            // Having unique participants, then grouping by company.
            .groupBy("companyName")
            .count();

        return intermediateDataFrame.orderBy(
                    intermediateDataFrame.col("count").desc(),
                    intermediateDataFrame.col("companyName").asc())
                .map(new ToParticipantsByCompanyFunction(), ClassTag$.MODULE$.<ParticipantsByCompany>apply(ParticipantsByCompany.class))
                .toJavaRDD()
                .collect();
    }

    @Override
    public List<EventsByParticipant> getMostActiveParticipants(boolean includeOnlyPresentParticipants) {
        DataFrame intermediateDataFrame = dataFrameDataFilesReader.readAllParticipants(includeOnlyPresentParticipants)
            .groupBy("email")
            .count();

        return intermediateDataFrame.orderBy(intermediateDataFrame.col("count").desc(), intermediateDataFrame.col("email").asc())
            .map(new ToEventsByParticipantFunction(), ClassTag$.MODULE$.<EventsByParticipant>apply(EventsByParticipant.class))
            .toJavaRDD()
            .collect();
    }

    @Override
    public List<ParticipantEmailPosition> getParticipantsByPosition(String position, boolean includeOnlyPresentParticipants) {
        DataFrame allParticipants = dataFrameDataFilesReader.readAllParticipants(includeOnlyPresentParticipants);
        Column positionColumn = allParticipants.col("position");
        Column emailColumn = allParticipants.col("email");

        return allParticipants.filter(
                positionColumn.contains(position).or(positionColumn.contains(WordUtils.capitalize(position))))
                .groupBy("email", "position")
                .count()
                .orderBy(emailColumn.asc(), positionColumn.asc())
                .map(new ToParticipantEmailPositionFunction(), ClassTag$.MODULE$.<ParticipantEmailPosition>apply(ParticipantEmailPosition.class))
                .toJavaRDD()
                .collect();
    }

    @Override
    public List<String> getUniqueParticipantsEmails(boolean includeOnlyPresentParticipants) {
        return dataFrameDataFilesReader.readAllParticipants(includeOnlyPresentParticipants)
            // Removing duplicates, one participant from the same company could attend few times.
            .groupBy("email")
            .count()
            .orderBy("email")
            .map(new ToParticipantEmailFunction(), ClassTag$.MODULE$.<String>apply(String.class))
            .toJavaRDD()
            .collect();
    }

    @Override
    public Long getParticipantsCount(boolean includeOnlyPresentParticipants) {
        return dataFrameDataFilesReader.readAllParticipants(includeOnlyPresentParticipants)
            .groupBy("email")
            .count()
            .count();
    }

}
