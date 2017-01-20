package com.lohika.morning.spark.presentation.api.service;

import com.lohika.morning.spark.presentation.api.ApplicationConfiguration;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.EventsByParticipant;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantEmailPosition;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantsByCompany;
import com.lohika.morning.spark.presentation.spark.driver.context.AnalyticsSparkContext;
import java.util.List;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ApplicationConfiguration.class, ApplicationConfigurationTest.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class AnalyticsServiceConsistencyTest {

    public static final int TOTAL_PARTICIPANTS = 426;
    public static final int PRESENT_PARTICIPANTS = 300;

    @Autowired
    private AnalyticsSparkContext analyticsSparkContext;

    @Autowired
    private AnalyticsServiceRDDImplementation rddImplementation;

    @Autowired
    private AnalyticsServiceDatasetDSLImplementation dslImplementation;

    @Autowired
    private AnalyticsServiceDatasetSQLImplementation sqlImplementation;

    @After
    public void tearDown() {
        analyticsSparkContext.getJavaSparkContext().stop();
    }

    @Test
    public void shouldGetTheSameParticipantsByCompanies() {
        List<ParticipantsByCompany> participantsByCompaniesRDDResult = rddImplementation
            .getParticipantsByCompanies(false);

        List<ParticipantsByCompany> participantsByCompaniesDSLResult = dslImplementation
            .getParticipantsByCompanies(false);

        List<ParticipantsByCompany> participantsByCompaniesSQLResult = sqlImplementation
            .getParticipantsByCompanies(false);

        assertThat(93, allOf(is(participantsByCompaniesRDDResult.size()),
                             is(participantsByCompaniesDSLResult.size()),
                             is(participantsByCompaniesDSLResult.size())));
        assertEquals(participantsByCompaniesRDDResult, participantsByCompaniesDSLResult);
        assertEquals(participantsByCompaniesRDDResult, participantsByCompaniesSQLResult);
    }

    @Test
    public void shouldGetTheSameUniqueParticipantsEmails() {
        List<String> uniqueParticipantsEmailsRDDResult = rddImplementation.getUniqueParticipantsEmails(false);
        List<String> uniqueParticipantsEmailsDSLResult = dslImplementation.getUniqueParticipantsEmails(false);
        List<String> uniqueParticipantsEmailsSQLResult = sqlImplementation.getUniqueParticipantsEmails(false);

        assertThat(TOTAL_PARTICIPANTS, allOf(is(uniqueParticipantsEmailsRDDResult.size()),
                is(uniqueParticipantsEmailsDSLResult.size()),
                is(uniqueParticipantsEmailsSQLResult.size())));

        assertEquals(uniqueParticipantsEmailsRDDResult, uniqueParticipantsEmailsDSLResult);
        assertEquals(uniqueParticipantsEmailsRDDResult, uniqueParticipantsEmailsSQLResult);
    }

    @Test
    public void shouldGetTheSameMostActiveParticipants() {
        List<EventsByParticipant> eventsByParticipantsRDDResult = rddImplementation.getMostActiveParticipants(false);
        List<EventsByParticipant> eventsByParticipantsDSLResult = dslImplementation.getMostActiveParticipants(false);
        List<EventsByParticipant> eventsByParticipantsSQLResult = sqlImplementation.getMostActiveParticipants(false);

        assertThat(TOTAL_PARTICIPANTS, allOf(is(eventsByParticipantsRDDResult.size()),
                              is(eventsByParticipantsDSLResult.size()),
                              is(eventsByParticipantsSQLResult.size())));

        assertEquals(eventsByParticipantsRDDResult, eventsByParticipantsDSLResult);
        assertEquals(eventsByParticipantsRDDResult, eventsByParticipantsSQLResult);
    }

    @Test
    public void shouldGetTheSameParticipantsByPosition() {
        List<ParticipantEmailPosition> participantsByPositionRDDResult = rddImplementation.getParticipantsByPosition("lead", false);
        List<ParticipantEmailPosition> participantsByPositionDSLResult = dslImplementation.getParticipantsByPosition("lead", false);
        List<ParticipantEmailPosition> participantsByPositionSQLResult = sqlImplementation.getParticipantsByPosition("lead", false);

        assertThat(43, allOf(is(participantsByPositionRDDResult.size()),
                             is(participantsByPositionDSLResult.size()),
                             is(participantsByPositionSQLResult.size())));

        assertEquals(participantsByPositionRDDResult, participantsByPositionDSLResult);
        assertEquals(participantsByPositionRDDResult, participantsByPositionSQLResult);
    }

    @Test
    public void shouldGetTheSameParticipantsCount() {
        Long participantsCountRDDResult = rddImplementation.getParticipantsCount(false);
        Long participantsCountDSLResult = dslImplementation.getParticipantsCount(false);
        Long participantsCountSQLResult = sqlImplementation.getParticipantsCount(false);

        assertThat((long) TOTAL_PARTICIPANTS, allOf(is(participantsCountRDDResult),
                              is(participantsCountDSLResult),
                              is(participantsCountSQLResult)));

        assertEquals(participantsCountRDDResult, participantsCountDSLResult);
        assertEquals(participantsCountRDDResult, participantsCountSQLResult);
    }

    @Test
    public void shouldGetTheSamePresentParticipantsCount() {
        Long participantsCountRDDResult = rddImplementation.getParticipantsCount(true);
        Long participantsCountDSLResult = dslImplementation.getParticipantsCount(true);
        Long participantsCountSQLResult = sqlImplementation.getParticipantsCount(true);

        assertThat((long) PRESENT_PARTICIPANTS, allOf(is(participantsCountRDDResult),
                is(participantsCountDSLResult),
                is(participantsCountSQLResult)));

        assertEquals(participantsCountRDDResult, participantsCountDSLResult);
        assertEquals(participantsCountRDDResult, participantsCountSQLResult);
    }

}
