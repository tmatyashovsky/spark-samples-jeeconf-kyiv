package com.lohika.morning.spark.presentation.spark.driver.function.rdd;

import com.lohika.morning.spark.presentation.spark.driver.reader.RDDDataFilesReader;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.map.ToParticipantsByCompanyFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.pair.PairParticipantCompanyNameCountFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.pair.PairParticipantEmailCompanyNameCountFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.pair.PairParticipantsCountByCompanyFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.reduce.ReduceBySumValueFunction;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantsByCompany;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ParticipantsByCompaniesFunction {

    @Autowired
    private RDDDataFilesReader dataFilesReader;

    public List<ParticipantsByCompany> getData(final boolean includeOnlyPresentParticipants) {
        return dataFilesReader.readAllParticipants(includeOnlyPresentParticipants)
            .mapToPair(new PairParticipantEmailCompanyNameCountFunction())             // Preparing email/company name as key with count 1.
            .groupByKey()
            // Unique combinations of email/company name.
            .keys()
            // Taking all companies.
            .mapToPair(new PairParticipantCompanyNameCountFunction())                  // Preparing company name as key with count 1.
            .reduceByKey(new ReduceBySumValueFunction())
            .sortByKey()
            // Reducing by company name and summarizing the counts.
            .mapToPair(new PairParticipantsCountByCompanyFunction())                   // Reversing the pair in order to sort.
            .sortByKey(false)                                                          // Sorting for companies with top participants.
            .map(new ToParticipantsByCompanyFunction())                                // Mapping to required output format.
            .collect();
    }

}
