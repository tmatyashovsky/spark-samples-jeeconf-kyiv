package com.lohika.morning.spark.presentation.api.service;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.EventsByParticipant;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantEmailPosition;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantsByCompany;
import com.lohika.morning.spark.presentation.spark.driver.function.rdd.*;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component(value = "analyticsServiceRDD")
public class AnalyticsServiceRDDImplementation implements AnalyticsService {

    @Autowired
    private ParticipantsByCompaniesFunction participantsByCompaniesFunction;

    @Autowired
    private EventsByParticipantsFunction eventsByParticipantsFunction;

    @Autowired
    private ParticipantsByPositionFunction participantsByPositionFunction;

    @Autowired
    private ParticipantsEmailsFunction participantsEmailsFunction;

    @Autowired
    private ParticipantsCountFunction participantsCountFunction;

    public List<ParticipantsByCompany> getParticipantsByCompanies(final boolean includeOnlyPresentParticipants) {
        return participantsByCompaniesFunction.getData(includeOnlyPresentParticipants);
    }

    public List<EventsByParticipant> getMostActiveParticipants(final boolean includeOnlyPresentParticipants) {
        return eventsByParticipantsFunction.getData(includeOnlyPresentParticipants);
    }

    public List<ParticipantEmailPosition> getParticipantsByPosition(final String position, final boolean includeOnlyPresentParticipants) {
        return participantsByPositionFunction.getData(position, includeOnlyPresentParticipants);
    }

    public List<String> getUniqueParticipantsEmails(final boolean includeOnlyPresentParticipants) {
        return participantsEmailsFunction.getData(includeOnlyPresentParticipants);
    }

    @Override
    public Long getParticipantsCount(boolean includeOnlyPresentParticipants) {
        return participantsCountFunction.getData(includeOnlyPresentParticipants);
    }

}
