package com.lohika.morning.spark.presentation.spark.driver.function.rdd;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.EventsByParticipant;
import java.util.List;
import junit.framework.TestCase;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.assertEquals;

public class EventsByParticipantsFunctionTest extends FunctionTest {

    @Autowired
    private EventsByParticipantsFunction eventsByParticipantsFunction;

    @Test
    public void shouldGetEventsByParticipants() {
        List<EventsByParticipant> eventsByParticipants =
            eventsByParticipantsFunction.getData(false);

        TestCase.assertNotNull(eventsByParticipants);
        assertEquals(6, eventsByParticipants.size());
        assertEquals(2, eventsByParticipants.get(0).getEventsCount().longValue());
        assertEquals("mikalai.alimenkou@xpinjection.com", eventsByParticipants.get(0).getEmail());
    }

}
