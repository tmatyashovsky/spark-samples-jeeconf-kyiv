package com.lohika.morning.spark.presentation.spark.driver.function.rdd;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantEmailPosition;
import java.util.List;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ParticipantsByPositionFunctionTest extends FunctionTest {

    @Autowired
    private ParticipantsByPositionFunction participantsByPositionFunction;

    @Test
    public void shouldGetParticipantsByLeadPosition() {
        List<ParticipantEmailPosition> participantsByPosition =
                participantsByPositionFunction.getData("lead", false);

        assertNotNull(participantsByPosition);
        assertEquals(4, participantsByPosition.size());
    }

    @Test
    public void shouldGetParticipantsByEngineerPosition() {
        List<ParticipantEmailPosition> participantsByPosition =
                participantsByPositionFunction.getData("engineer", false);

        assertNotNull(participantsByPosition);
        assertEquals(2, participantsByPosition.size());
    }

}
