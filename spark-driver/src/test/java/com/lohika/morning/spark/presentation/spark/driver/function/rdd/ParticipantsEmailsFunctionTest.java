package com.lohika.morning.spark.presentation.spark.driver.function.rdd;

import java.util.List;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ParticipantsEmailsFunctionTest extends FunctionTest {

    @Autowired
    private ParticipantsEmailsFunction participantsEmailsFunction;

    @Test
    public void shouldGetParticipantsEmails() {
        List<String> participantsEmails =
                participantsEmailsFunction.getData(false);

        assertNotNull(participantsEmails);
        assertEquals(6, participantsEmails.size());
    }

}
