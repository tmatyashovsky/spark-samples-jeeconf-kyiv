package com.lohika.morning.spark.presentation.spark.driver.function.rdd;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantsByCompany;
import java.util.List;
import junit.framework.TestCase;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.assertEquals;

public class ParticipantsByCompaniesFunctionTest extends FunctionTest {

    @Autowired
    private ParticipantsByCompaniesFunction participantsByCompaniesFunction;

    @Test
    public void shouldGetParticipantsByCompanies() {
        List<ParticipantsByCompany> participantsByCompanies =
            participantsByCompaniesFunction.getData(false);

        TestCase.assertNotNull(participantsByCompanies);
        assertEquals(4, participantsByCompanies.size());
        assertEquals("Lohika", participantsByCompanies.get(0).getCompanyName());
        assertEquals(3, participantsByCompanies.get(0).getParticipantsCount().longValue());
    }

}
