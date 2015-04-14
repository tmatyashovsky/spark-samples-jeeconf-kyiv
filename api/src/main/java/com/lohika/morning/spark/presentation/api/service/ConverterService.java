package com.lohika.morning.spark.presentation.api.service;

import com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.map.FileContentToFileNameFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.map.FileContentToParticipantRowFunction;
import com.lohika.morning.spark.presentation.spark.driver.context.AnalyticsSparkContext;
import com.lohika.morning.spark.presentation.spark.driver.context.AnalyticsSqlSparkContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ConverterService {

    @Autowired
    private AnalyticsSqlSparkContext analyticsSqlSparkContext;

    @Autowired
    private AnalyticsSparkContext analyticsSparkContext;

    public void convertAllEventFilesToParquetFormat() {
        JavaPairRDD<String, String> rawData = analyticsSparkContext.readRawData();

        List<String> eventFileNames = rawData.map(new FileContentToFileNameFunction()).collect();

        for (String eventFileName: eventFileNames) {
            JavaRDD<Row> participantsAsRows = analyticsSparkContext.getJavaSparkContext().textFile(eventFileName)
                .map(new FileContentToParticipantRowFunction());

            DataFrame participantsAsDataFrame = analyticsSqlSparkContext.getSqlContext()
                .createDataFrame(participantsAsRows, generateRowSchemaStructure());

            String eventFileNameWithExtension = FilenameUtils.removeExtension(eventFileName);

            participantsAsDataFrame.saveAsParquetFile(eventFileNameWithExtension + ".parquet");
        }
    }

    /**
     * Generates Spark-based structures using hardcoded meta data.
     *
     * @return Spark-based structures
     */
    private StructType generateRowSchemaStructure() {
        // TODO: meta data can be read from external meta data file if needed.
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("firstName", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("lastName", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("companyName", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("position", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("email", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("present", DataTypes.BooleanType, false));

        return DataTypes.createStructType(fields);
    }
}
