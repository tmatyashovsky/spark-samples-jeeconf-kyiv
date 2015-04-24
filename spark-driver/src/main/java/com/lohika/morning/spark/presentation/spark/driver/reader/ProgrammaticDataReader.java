package com.lohika.morning.spark.presentation.spark.driver.reader;

import com.lohika.morning.spark.presentation.spark.driver.context.AnalyticsSparkContext;
import com.lohika.morning.spark.presentation.spark.driver.context.AnalyticsSqlSparkContext;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.map.FileContentToParticipantRowsFunction;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("programmaticDataReader")
public class ProgrammaticDataReader implements DataFrameDataReader {

    @Autowired
    private AnalyticsSqlSparkContext analyticsSqlSparkContext;

    @Autowired
    private AnalyticsSparkContext analyticsSparkContext;

    @Override
    public DataFrame readAllParticipants(boolean includeOnlyPresentParticipants) {
        JavaRDD<Row> participantsRowRDD = getParticipantsRowRDD(includeOnlyPresentParticipants);

        return createDataFrame(participantsRowRDD);
    }

    private JavaRDD<Row> getParticipantsRowRDD(final boolean includeOnlyPresentParticipants) {
        return analyticsSparkContext.readRawData()
                    .flatMap(new FileContentToParticipantRowsFunction(includeOnlyPresentParticipants));
    }

    private DataFrame createDataFrame(JavaRDD<Row> participantsRowRDD) {
        StructType schema = generateRowSchemaStructure();

        DataFrame participantsAsDataFrames = analyticsSqlSparkContext.getSqlContext().createDataFrame(participantsRowRDD, schema);
        // In case we would like to execute traditional SQL.
        participantsAsDataFrames.registerTempTable("participants");

        return participantsAsDataFrames;
    }

    /**
     * Generates Spark-based structures using hardcoded meta data.
     *
     * @return Spark-based structures
     */
    private StructType generateRowSchemaStructure() {
        // Meta data can be read from external meta data file if needed.
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("firstName", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("lastName", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("companyName", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("position", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("email", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("present", DataTypes.BooleanType, false));

        return DataTypes.createStructType(fields);
    }

    @Override
    public AnalyticsSqlSparkContext getAnalyticsSqlSparkContext() {
        return analyticsSqlSparkContext;
    }
}
