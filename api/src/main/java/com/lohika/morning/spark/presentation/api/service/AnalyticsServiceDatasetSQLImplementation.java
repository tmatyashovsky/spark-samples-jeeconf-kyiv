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
import org.apache.commons.lang.WordUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.stereotype.Component;

@Component(value = "analyticsServiceDataFrameSQL")
public class AnalyticsServiceDatasetSQLImplementation implements AnalyticsService {

    @Resource(name = "${datasetDataFilesReader}")
    private DatasetDataReader datasetDataFilesReader;

    @Override
    public List<ParticipantsByCompany> getParticipantsByCompanies(boolean includeOnlyPresentParticipants) {
        // SQL can be run over RDDs that have been registered as tables.
        datasetDataFilesReader.readAllParticipants(includeOnlyPresentParticipants);

        SQLContext sqlContext = datasetDataFilesReader.getAnalyticsSqlSparkContext().getSqlContext();

        Dataset<Row> uniqueParticipantsByCompanies = sqlContext.sql("select email, companyName, count(companyName) as duplicatesByParticipant " +
                "from participants " +
                        "group by companyName, email");

        uniqueParticipantsByCompanies.registerTempTable("uniqueParticipantsByCompanies");

        Dataset<Row> results = sqlContext.sql(
                "select companyName, count(companyName) as countByCompany " +
                        "from uniqueParticipantsByCompanies " +
                        "group by companyName " +
                        "order by countByCompany desc, companyName asc");

        return results.map(new ToParticipantsByCompanyFunction(), Encoders.bean(ParticipantsByCompany.class))
                .toJavaRDD()
                .collect();
    }

    @Override
    public List<EventsByParticipant> getMostActiveParticipants(boolean includeOnlyPresentParticipants) {
        // SQL can be run over RDDs that have been registered as tables.
        datasetDataFilesReader.readAllParticipants(includeOnlyPresentParticipants);

        SQLContext sqlContext = datasetDataFilesReader.getAnalyticsSqlSparkContext().getSqlContext();

        Dataset<Row> results = sqlContext.sql(
                "select email, count(email) as countByEmail " +
                        "from participants " +
                        "group by email " +
                        "order by countByEmail desc, email asc");

        return results.map(new ToEventsByParticipantFunction(), Encoders.bean(EventsByParticipant.class))
                .toJavaRDD()
                .collect();
    }

    @Override
    public List<ParticipantEmailPosition> getParticipantsByPosition(String position, boolean includeOnlyPresentParticipants) {
        // SQL can be run over RDDs that have been registered as tables.
        datasetDataFilesReader.readAllParticipants(includeOnlyPresentParticipants);

        SQLContext sqlContext = datasetDataFilesReader.getAnalyticsSqlSparkContext().getSqlContext();

        Dataset<Row> results = sqlContext.sql(
                "select email, position from participants " +
                        "group by email, position " +
                        "having position like \"%" + position + "%\"" +
                            " or position like \"%" + WordUtils.capitalize(position) + "%\" " +
                        "order by email asc, position asc");

        return results.map(new ToParticipantEmailPositionFunction(), Encoders.bean(ParticipantEmailPosition.class))
                .toJavaRDD()
                .collect();
    }

    @Override
    public List<String> getUniqueParticipantsEmails(boolean includeOnlyPresentParticipants) {
        // SQL can be run over RDDs that have been registered as tables.
        datasetDataFilesReader.readAllParticipants(includeOnlyPresentParticipants);

        SQLContext sqlContext = datasetDataFilesReader.getAnalyticsSqlSparkContext().getSqlContext();

        Dataset<Row> results = sqlContext.sql(
                "select email, count(email) as countByEmail from participants " +
                        "group by email order by email asc");

        return results.map(new ToParticipantEmailFunction(), Encoders.bean(String.class))
                .toJavaRDD()
                .collect();
    }

    @Override
    public Long getParticipantsCount(boolean includeOnlyPresentParticipants) {
        // SQL can be run over RDDs that have been registered as tables.
        datasetDataFilesReader.readAllParticipants(includeOnlyPresentParticipants);

        SQLContext sqlContext = datasetDataFilesReader.getAnalyticsSqlSparkContext().getSqlContext();

        Dataset<Row> results = sqlContext.sql(
                "select sum(email) from participants group by email");

        return results.toJavaRDD().count();
    }

}
