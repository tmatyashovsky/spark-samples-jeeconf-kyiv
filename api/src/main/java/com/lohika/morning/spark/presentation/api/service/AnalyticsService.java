package com.lohika.morning.spark.presentation.api.service;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.EventsByParticipant;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantEmailPosition;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantsByCompany;
import java.util.List;

public interface AnalyticsService {

    List<ParticipantsByCompany> getParticipantsByCompanies(final boolean includeOnlyPresentParticipants);

    List<EventsByParticipant> getMostActiveParticipants(final boolean includeOnlyPresentParticipants);

    List<ParticipantEmailPosition> getParticipantsByPosition(final String position, final boolean includeOnlyPresentParticipants);

    List<String> getUniqueParticipantsEmails(final boolean includeOnlyPresentParticipants);

    Long getParticipantsCount(final boolean includeOnlyPresentParticipants);

}
